/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections.glb;

import static apgas.Constructs.*;
import static apgas.ExtendedConstructs.*;
import static handist.collections.glb.GlobalLoadBalancer.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import apgas.ExtendedConstructs;
import apgas.GlobalRuntime;
import apgas.Place;
import apgas.impl.Finish;
import apgas.util.PlaceLocalObject;
import handist.collections.Bag;
import handist.collections.dist.DistLog;
import handist.collections.dist.DistributedCollection;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.glb.lifeline.Lifeline;
import handist.collections.glb.lifeline.LifelineFactory;
import handist.collections.glb.lifeline.Loop;

/**
 * Distributed object in charge of handling the GLB runtime and the work
 * stealing and between hosts.
 *
 * @author Patrick Finnerty
 *
 */
class GlbComputer extends PlaceLocalObject {

    /**
     * Class representing the fact that a host wants to steal some work from another
     * host.
     *
     * @author Patrick Finnerty
     *
     */
    final static class LifelineToken implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = 1445594155772627013L;
        /** Distributed collection from which assignments are desired */
        @SuppressWarnings("rawtypes")
        DistributedCollection collection;
        /**
         * Place performing the steal / answering a lifeline steal
         */
        Place place;

        /**
         * Unique Long Identifier of the operation source of this lifeline steal final
         */
        long batchId;

        /**
         * Constructor for a lifeline token. The target collection and the source of the
         * steal are specified as parameters
         *
         * @param c distributed collection from which work is desired
         * @param p place which is trying to steal some work
         */
        @SuppressWarnings("rawtypes")
        private LifelineToken(GlbOperation op, Place p, long batchNumber) {
            collection = op.collection;
            place = p;
            batchId = batchNumber;

        }

        @Override
        public String toString() {
            return "LifelineToken[" + place + "/" + collection + "]";
        }
    }

    /**
     * Class containing information and logging facilities of an individual worker.
     * As part of the initialization process for the GLB, an instance of this class
     * is prepared for each concurrent worker that may run on the host.
     *
     * @author Patrick Finnerty
     *
     */
    final class WorkerInfo implements WorkerService {

        /**
         * Counts the number of times this worker split its assignment to increase
         * parallelism on the host
         */
        int assignmentSplit;

        /**
         * Counts the number of times this worker tried to split its assignment but
         * failed to do so as the assignment it held was too small
         */
        int assignmentUnabledToSplit;

        /** Current operation being processed by this worker */
        @SuppressWarnings("rawtypes")
        GlbOperation currentOperation;

        /** Unique integer identifier */
        final int id;

        /**
         * Number of times the worker made an answer to a remote thief.
         */
        public int lifelineAnswer;

        /**
         * Number of times the worker attempted to make an answer to a lifeline thief
         * but this attempt failed because no assignments were available on the local
         * host.
         */
        public int lifelineCannotAnswer;

        /**
         * Time (in nanoseconds) spent working by this worker. The time spent yielding
         * is not included.
         */
        public long timeWorking;

        /** Time spent yielding to other tasks (not counted in {@link #timeWorking}) */
        public long timeYielding;

        /**
         * Counts the number of times this worker successfully takes an assignment from
         * the reserve for itself
         */
        int tookFromReserve;

        /**
         * In some operations, an object may need to remain bound to a single worker and
         * used throughout the processing of the operation. In such a case, this objects
         * is kept in this collection.
         */
        final Map<Object, Object> workerBoundObjects;

        /**
         * Counts the number of times this worker was spawned
         */
        int workerSpawned;

        /**
         * Constructor
         * <p>
         * The unique identifier of the instance created needs to be specified as
         * parameter.
         *
         * @param workerId identifier for the object to create
         */
        public WorkerInfo(int workerId) {
            id = workerId;
            workerBoundObjects = new ConcurrentHashMap<>();
        }

        @Override
        public void attachOperationObject(Object key, Object o) {
            workerBoundObjects.put(key, o);
        }

        @Override
        public Object retrieveOperationObject(Object key) {
            return workerBoundObjects.get(key);
        }

        @Override
        public void throwableInOperation(Throwable t) {
            @SuppressWarnings("rawtypes")
            final Map<GlbOperation, ArrayList<Throwable>> errors = getComputer().operationErrors;

            synchronized (errors) {
                errors.computeIfAbsent(currentOperation, k -> new ArrayList<>()).add(t);
            }
        }
    }

    /**
     * Class used on each host to hold the work from which workers and remote steals
     * take from.
     * <p>
     * Internally it manages the (multiple) {@link GlbTask} instances that are being
     * processes on the host. Workers obtain pieces of work through an instance of
     * this class
     *
     * @author Patrick Finnerty
     *
     */
    final class WorkReserve {
        /**
         * Lock used to guarantee availability of {@link WorkerInfo} instances inside
         * the {@link #idleWorkers} collection when they fail to take some work from
         * this instance.
         */
        final ReadWriteLock lock;

        /**
         * Map from distributed collection to their respective GlbTask instance. This
         * map is used to keep track of distributed collections that may already have an
         * operation in progress. If that is the case and an additional operation comes
         * along, this operation will need to be added to the existing {@link GlbTask}
         * of this distributed collection.
         */
        @SuppressWarnings("rawtypes")
        Map<DistributedCollection, GlbTask> allTasks;

        /**
         * Map which contains the operation with work left as keys and the GlbTask is in
         * charge of handling the corresponding assignments. As all the assignments of a
         * certain operations are completed, the mapping in this collection will be
         * removed at the GlbTask's initiative as part of the
         * {@link Assignment#process(int)} method. This map is traversed when workers
         * try to acquire a new assignment from the reserve in method
         * {@link #getAssignment()}.
         */
        @SuppressWarnings("rawtypes")
        ConcurrentSkipListMap<GlbOperation, GlbTask> tasksWithWork;

        /**
         * Constructor
         * <p>
         * Prepares the members of WorkReserve to receive the various GLB operations
         */
        private WorkReserve() {
            lock = new ReentrantReadWriteLock();
            allTasks = new HashMap<>();
            tasksWithWork = new ConcurrentSkipListMap<>();
        }

        /**
         * Checks the GlbTask of the current host to provide some work to a worker.
         * <p>
         * If at the time this method is called no work could be selected, returns null
         * instead.
         * <p>
         * This implementation uses the natural ordering of class {@link GlbOperation}
         * in a {@link ConcurrentSkipListMap} to favor the higher priority
         * {@link GlbOperation}s.
         *
         * @param wInfo the worker info instance which will be placed back into the
         *              #idleWorkers collection if calling this method did not result in
         *              an Assignment being given to the worker
         * @return the provided instance with updates, or null if no work could be
         *         obtained
         */
        Assignment getAssignment(WorkerInfo wInfo) {
            lock.readLock().lock();
            for (final GlbTask t : tasksWithWork.values()) {
                Assignment a;
                if ((a = t.assignWorkToWorker()) != null) {
                    lock.readLock().unlock();
                    return a;
                }
            }
            // Unable to get an assignment for the worker
            idleWorkers.add(wInfo);
            lock.readLock().unlock();

            reserveWasEmptied();
            return null;
        }

        /**
         * Registers a new operation in the local reserve
         *
         * @param op operation newly available to workers
         * @return true if some work was actually made available to workers as a result
         *         of this operation
         */
        public synchronized boolean newOperation(GlbOperation<?, ?, ?, ?, ?, ?> op) {
            // There is a GlbTask instance for the underlying collection (either created or
            // retrieved)
            GlbTask toPlaceInAvailable;
            if (!allTasks.containsKey(op.collection)) {
                // We need to create a new GlbTask for the target collection
                toPlaceInAvailable = op.initializerOfGlbTask.get();
                allTasks.put(op.collection, toPlaceInAvailable);
            } else {
                // We "add" the operation to the existing instance
                toPlaceInAvailable = allTasks.get(op.collection);
            }

            final boolean workCreated = toPlaceInAvailable.newOperations(op);
            if (workCreated) {
                // The GlbTask may already be present in tasksWithWork if another operation is
                // being processed already. HashSet#add will keep the tasksWithWork set
                // unchanged in this case.
                tasksWithWork.put(op, toPlaceInAvailable);
            }
            return workCreated;
        }

        public synchronized boolean newOperationBatch(GlbOperation<?, ?, ?, ?, ?, ?>[] operationArray) {
            final DistributedCollection<?, ?> col = operationArray[0].collection;
            GlbTask toPlaceInAvailable;
            if (!allTasks.containsKey(col)) {
                // FIXME the initializer for the GlbTask should be obtained from the distributed
                // collection itself
                toPlaceInAvailable = operationArray[0].initializerOfGlbTask.get();
                allTasks.put(col, toPlaceInAvailable);
            } else {
                toPlaceInAvailable = allTasks.get(col);
            }

            // Variable to value to return
            final boolean workCreated = toPlaceInAvailable.newOperations(operationArray);
            if (workCreated) {
                for (final GlbOperation<?, ?, ?, ?, ?, ?> op : operationArray) {
                    tasksWithWork.put(op, toPlaceInAvailable);
                }
            }

            return workCreated;
        }

        /**
         * Discards all tracking information kept until that point. Can only be called
         * safely if there are no ongoing GLB computation.
         *
         * @see GlobalLoadBalancer#reset()
         */
        public void reset() {
            allTasks.clear();
            tasksWithWork.clear();
        }
    }

    /** Singleton, local handle instance */
    private static GlbComputer computer = null;

    /**
     * Code used in member {@link #lifelineEstablished} to represent the fact that
     * this place had a lifeline established with the remote host
     *
     * @see #establishingLifelineOnRemoteHost(DistributedCollection)
     */
    private static final int LIFELINE_ESTABLISHED = 1;

    /**
     * Code used in member {@link #lifelineEstablished} to represent the fact that
     * this place has not established a lifeline with a remote host
     *
     * @see #establishingLifelineOnRemoteHost(DistributedCollection)
     */
    private static final int LIFELINE_NOT_ESTABLISHED = 0;

    /**
     * Setting describing if tracing is activated. If so, a number of output
     * messages are made to {@link System#err} as GLB events occur.
     */
    static boolean TRACE = Boolean.parseBoolean(System.getProperty(Config.ACTIVATE_TRACE, "false"));

    /**
     * Destroys the GlbComputer singleton on all places. This method should be
     * called before calling method {@link #initializeComputer(DistLog)} or
     * {@link #getComputer()}
     */
    static void destroyGlbComputer() {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            computer = null;
        });
    }

    /**
     * Method returning the local singleton for GlbComputer. May return null if
     * method {@link #initializeComputer(DistLog)} was not called beforehand.
     *
     * @return GlbComputer local handle
     */
    static GlbComputer getComputer() {
        assertNotNull(computer);
        return computer;
    }

    /**
     * Performs the global initialization of a GlbComputer instance on all hosts.
     * Any previous instance must be destroyed before calling this method.
     *
     * @param log the logger instance into which all the event that occur as part of
     *            the GLB program eecution will be recorded
     * @return the GlbComputer instance created locally
     */
    static GlbComputer initializeComputer(DistLog log) {
        return PlaceLocalObject.make(TeamedPlaceGroup.getWorld().places(), () -> {
            final GlbComputer c = new GlbComputer(log);
            // Assign the static member of class GlbComputer here
            // Doing so here avoids the need for a second dedicated finish/asyncAt block
            if (computer == null) {
                GlbComputer.computer = c;
                if (TRACE) {
                    System.err.println("GlbComputer on " + here() + " is " + c);
                }
            } else {
                throw new IllegalStateException("Previous GlbComputer was not destroyed on " + here());
            }
            return c;
        });
    }

    /**
     * Map associating every GlbOperation with the semaphore used to block the
     * thread representing the presence of work for this operation.
     * <p>
     * This member needs to be accessed through a synchronized block as it is not
     * protected against concurrent modifications. As it is not accessed often, the
     * use of a concurrent data structure is not warranted.
     */
    @SuppressWarnings("rawtypes")
    private final Map<GlbOperation, OperationBlocker> blockers;

    /**
     * Atomic array used to signal to workers that they need to place some work back
     * into the {@link #reserve}. Each worker is identified with a unique index.
     * They use this identifier to access this array which is initialized with an
     * initial length of {@link #MAX_WORKERS}.
     * <p>
     * The value used in this array are:
     * <ul>
     * <li>0: the worker can move on in its {@link #worker(Integer, Assignment)}
     * procedure's main loop, the {@link #reserve} does not need to be fed with work
     * <li>1: the worker should feed the {@link #reserve} in its main loop before
     * setting its value back to 0.
     * </ul>
     */
    private final AtomicIntegerArray feedReserveRequested;

    /**
     * Map associating every GlbOperation with the finish instance in which they are
     * being executed
     */
    /*
     * This member needs to be protected against concurrent modifications as there
     * is a risk that multiple starting operations may try to insert their "finish"
     * value inside this collection concurrently. While we could use synchronized
     * blocks for all accesses made to this object, it would be error-prone as
     * workers making lifeline answer may access this object at any time from
     * outside this object. A concurrent data collection was therefore preferred.
     */
    @SuppressWarnings("rawtypes")
    final ConcurrentHashMap<GlbOperation, Finish> finishes;

    /**
     * Long used to keep track of the batch number with respect to each distributed
     * collection.
     * <p>
     * Each time a new batch of operations is started, the counter for the targeted
     * collection in this collection is incremented. This information is then used
     * by the workers to determine if the lifeline tokens present in
     * {@link #lifelineThieves} are relevant or not.
     */
    @SuppressWarnings("rawtypes")
    final ConcurrentHashMap<DistributedCollection, Long> currentBatch;

    /**
     * Number of elements processed by workers in one gulp before they check the
     * runtime
     */
    volatile int granularity;

    /**
     * Concurrent linked list which contains the inactive workers that may run on
     * the host if they are given an assignment.
     */
    private final ConcurrentLinkedDeque<WorkerInfo> idleWorkers;

    /**
     * Collection used to keep track of the lifelines that are established on remote
     * hosts
     */
    @SuppressWarnings("rawtypes")
    ConcurrentHashMap<DistributedCollection, ConcurrentHashMap<Place, AtomicInteger>> lifelineEstablished;

    /**
     * Lifeline requests for work coming from remote places in the system.
     * <p>
     * A lifeline request consists in a token which contains the distributed
     * collection which is the target of the steal and the Place which requires the
     * work. Refer to
     */
    ConcurrentLinkedQueue<LifelineToken> lifelineThieves;

    /**
     * Logger for the events occurring on this host
     */
    DistLog logger;

    /**
     * Maximum number of workers that can concurrently run on the local host
     */
    final int MAX_WORKERS;

    /**
     * Member used for book-keeping of the errors that occur during GLB operations.
     */
    @SuppressWarnings("rawtypes")
    final transient Map<GlbOperation, ArrayList<Throwable>> operationErrors;

    /** Fork Join Pool used by the APGAS runtime */
    final ForkJoinPool POOL;

    /**
     * Instance in which the workers on a host take / place work back into
     */
    WorkReserve reserve;

    /**
     * Collection of Locks which contains the locks that are available for workers
     * to pick up to actively yield to other activities.
     */
    private final ConcurrentLinkedQueue<TimeoutBlocker> workerAvailableLocks;

    /**
     * Array containing all the workers initialized on the host Contrary to
     * {@link #idleWorkers}, the contents of this array never change. This array is
     * used to access all the workers irrespective of if they are running or idle.
     * <p>
     * One particular case we need to access all workers is when an operation needs
     * to attach an object to each worker which is needed when this operation is
     * processed by a the workers.
     */
    final WorkerInfo[] workers;

    /**
     * Lock used to force workers to yield execution to allow other activities to
     * run
     */
    private final TimeoutBlocker workerYieldLock;

    /**
     * Private constructor
     * <p>
     * GlbComputer is a global object that follows a singleton design pattern. This
     * constructor is made private to protect this property.
     *
     * @param log the logger instance into which the events that occur on this host
     *            will be recorded throughout the execution
     */
    private GlbComputer(DistLog log) {
        // Set the constants related to runtime environment
        MAX_WORKERS = Config.getMaximumConcurrentWorkers();
        POOL = (ForkJoinPool) GlobalRuntime.getRuntime().getExecutorService();

        // Initialize the single lock used by workers to actively yield to other
        // activities (lifeline steals / other non-GLB activities)
        workerYieldLock = new TimeoutBlocker();
        workerAvailableLocks = new ConcurrentLinkedQueue<>();
        workerAvailableLocks.add(workerYieldLock);

        // Prepare the worker Ids
        workers = new WorkerInfo[MAX_WORKERS];
        idleWorkers = new ConcurrentLinkedDeque<>();
        for (int i = 0; i < MAX_WORKERS; i++) {
            final WorkerInfo w = new WorkerInfo(i);
            idleWorkers.add(w);
            workers[i] = w;
        }

        // Prepare the atomic array used as flag to signal to workers that the #reserve
        // needs to be fed
        feedReserveRequested = new AtomicIntegerArray(MAX_WORKERS);

        // Set an initial value for the granularity
        granularity = Config.getGranularity();

        // Initialize the reserve of assignments for this host
        reserve = new WorkReserve();

        // Initialize the map that will contain the errors that may occur during GLB
        // operation
        operationErrors = new HashMap<>();

        finishes = new ConcurrentHashMap<>();
        currentBatch = new ConcurrentHashMap<>();
        blockers = new HashMap<>();

        // Initialize the data structures used to keep track of the lifelines
        lifelineThieves = new ConcurrentLinkedQueue<>();
        lifelineEstablished = new ConcurrentHashMap<>();

        // Setup the logger instance for this local host
        logger = log;
        logger.put(LOGKEY_GLB, LOG_INITIALIZED_WORKERS, Integer.toString(MAX_WORKERS));
        logger.put(LOGKEY_GLB, LOG_INITIALIZED_AT_NANOTIME, Long.toString(System.nanoTime()));
    }

    /**
     * Attempt to spawn a worker (uncounted) with an assignment obtained from the
     * reserve. It is possible all the assignment this operation brought about were
     * already taken up by existing workers, preventing this thread from obtaining
     * work from the reserve. It is also possible that at the time this method is
     * called, there are already the maximum number of workers running, in which
     * case an additional worker is not spawned either. What is guaranteed by the
     * spawn here is that if there were no workers running and some work is
     * available, then a worker is spawned. In case workers are already running but
     * it is possible to spawn an extra one, then it is perfectly fine to spawn an
     * extra one.
     */
    private void attemptToSpawnWorker() {
        final WorkerInfo worker = idleWorkers.poll();
        if (worker != null) {
            // In case getAssignment was not able to deliver an assignment for our worker,
            // the worker instance is placed back into #idleWorkers as part of this method
            final Assignment forSpawn = reserve.getAssignment(worker);
            if (forSpawn != null) {
                if (TRACE) {
                    System.err.println(here() + " spawned worker(" + worker.id + ")");
                }
                uncountedAsyncAt(here(), () -> worker(worker, forSpawn));
            }
        }
    }

    /**
     * Procedure called by the thread representing the presence of work for a given
     * operation on this host when all the local assignments have completed and it
     * established lifelines on neighbor nodes to obtain some work.
     * <p>
     * As under our current implementation, lifelines are established on a "per
     * collection" basis, it is possible that an operation which terminates does not
     * actually establish any new lifelines. This is the case if multiple operations
     * on the same collection are ongoing and a previous operation terminated
     * before, establishing the lifelines before this new operation did.
     *
     * @param c the collection on which the operation that has terminated operated
     */
    void establishingLifelineOnRemoteHost(GlbOperation<?, ?, ?, ?, ?, ?> op) {
        final DistributedCollection<?, ?> c = op.collection;
        final LifelineToken token = new LifelineToken(op, here(), currentBatch.get(c).longValue());

        final ConcurrentHashMap<Place, AtomicInteger> lifelineStatus = lifelineEstablished.get(c);

        for (final Map.Entry<Place, AtomicInteger> pair : lifelineStatus.entrySet()) {
            // An atomic check is made to avoid establishing a lifeline redundantly
            if (pair.getValue().compareAndSet(LIFELINE_NOT_ESTABLISHED, LIFELINE_ESTABLISHED)) {
                // This thread set the flag to "established", it can now make the assynchronous
                // call that actually establishes the lifeline on the remote host

                asyncAt(pair.getKey(), () -> {
                    try {
                        if (TRACE) {
                            System.err.println(token.place + " established lifeline on " + here() + " for collection "
                                    + token.collection);
                        }
                        lifelineThieves.add(token);
                    } catch (final Exception e) {
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    /**
     * Procedure called by a remote host when giving some work as part of a lifeline
     * answer.
     *
     * @param token   token containing the information about the collection and the
     *                place making the answer
     * @param stolen  collection of assignments that will now be under this host
     *                responsibility
     * @param numbers the number of assignments with work that are given for each
     *                operation that the asignments may contain
     */
    void lifelineAnswer(final LifelineToken token, final ArrayList<Assignment> stolen,
            @SuppressWarnings("rawtypes") final HashMap<GlbOperation, Integer> numbers,
            @SuppressWarnings("rawtypes") final HashMap<GlbOperation, Finish> finish) {
        if (TRACE) {
            System.err.println(here() + " received " + stolen.size() + " assignments from " + token.place);
        }

        // The lifeline answer has just been received, we set the lifeline tracker back
        // to "not established"
        final ConcurrentHashMap<Place, AtomicInteger> lifelinesForCollection = lifelineEstablished
                .get(token.collection);
        final AtomicInteger state = lifelinesForCollection.get(token.place);
        final boolean resetLifeline = state.compareAndSet(LIFELINE_ESTABLISHED, LIFELINE_NOT_ESTABLISHED);
        assertTrue(resetLifeline); // Check that the previous operation worked properly

        final GlbTask glbTask = reserve.allTasks.get(token.collection);

        // Merge the assignments
        glbTask.mergeAssignments(numbers, stolen);

        /*
         * I know, this is weird. But allow me to explain. This guarantees that workers
         * which may have failed to take some work from the reserve (before assignments
         * from this lifeline answer were merged into it) have had their WorkerInfo
         * instance placed back into the #idleWorkers collection. See method
         * WorkReserve#getAssignment.
         *
         * In the rare (but not impossible) case where all the workers fail to get work
         * from the reserve just before this lifeline answer merges new assignments,
         * this guarantees that the calls to #attempToSpawnWorker that are made inside
         * method #operationActivity will find these WorkerInfo instances and be able to
         * spawn them.
         */
        reserve.lock.writeLock().lock();
        reserve.lock.writeLock().unlock();

        // For each operation that was transmitted, place an asynchronous task that will
        // wait till operation termination
        for (@SuppressWarnings("rawtypes")
        final GlbOperation op : numbers.keySet()) {
            final Finish f = finishes.get(op);
            reserve.tasksWithWork.put(op, glbTask);
            asyncArbitraryFinish(here(), () -> witnessActivity(op), f);
        }

    }

    @SuppressWarnings("rawtypes")
    void newOperationBatch(GlbOperation[] operationArray, Finish[] finishArray) {
        final DistributedCollection col = operationArray[0].collection;

        // Increment the batch counter for this collection
        currentBatch.compute(col, (k, v) -> v == null ? 0 : v + 1);

        // Prepare the data structure for tracking the state of the outgoing lifelines
        // if it was not already created.
        // FIXME the lifeline to use (if not the default) should be obtained from the
        // collection's GLB handle, not the GlbOperation themselves
        final GlbOperation op = operationArray[0];
        final Map<Place, AtomicInteger> colLifelines = lifelineEstablished.computeIfAbsent(col, c -> {
            final ConcurrentHashMap<Place, AtomicInteger> map = new ConcurrentHashMap<>();
            Lifeline l;
            try {
                if (op.lifelineClass == null) {
                    l = LifelineFactory.newLifeline(col.placeGroup());
                } else {
                    l = LifelineFactory.newLifeline(col.placeGroup(), op.lifelineClass);
                }
            } catch (final Exception e) {
                e.printStackTrace();
                System.err.println("Faced a " + e + " when attempting to initialize a new lifeline strategy. Using "
                        + Loop.class.getName() + " instead");
                l = new Loop(col.placeGroup());
            }
            for (final Place p : l.lifeline(here())) {
                map.put(p, new AtomicInteger());
            }
            return map;
        });
        // Reset the atomic flags to 0 for all the lifelines of this collection
        colLifelines.forEach((place, lifelineStatus) -> lifelineStatus.set(LIFELINE_NOT_ESTABLISHED));

        // Perform the initialization of the assignments with each operation's tracking
        // system, as well as the semaphores / updates to the finish tracking system
        final boolean localWorkCreated = prepareForNewBatch(operationArray, finishArray);

        // If work was created, spawn a witness activity for each operation in the batch
        if (localWorkCreated) {
            for (int i = 0; i < operationArray.length; i++) {
                final int ii = i;
                ExtendedConstructs.asyncArbitraryFinish(here(), () -> witnessActivity(operationArray[ii]),
                        finishArray[ii]);
            }
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private boolean prepareForNewBatch(GlbOperation[] operationArray, Finish[] finishArray) {
        synchronized (blockers) {
            // for each operation in the batch
            for (int i = 0; i < operationArray.length; i++) {
                final GlbOperation op = operationArray[i];
                final Finish f = finishArray[i];
                // Create the blocker for the "witness" activity
                blockers.put(op, new OperationBlocker());
                // Record the finish for that operation
                finishes.put(op, f);

                if (op.workerInit != null) {
                    for (final WorkerInfo wi : workers) {
                        op.workerInit.accept(wi);
                    }
                }
            }
        }

        return reserve.newOperationBatch(operationArray);
    }

    /**
     * Helper procedure used to signal to all workers that they need to place some
     * work back into the {@link #reserve} if they can.
     * <p>
     * This procedure is called by whichever worker notices the fact the
     * {@link #reserve} got empty first. It may be called by multiple workers
     * concurrently without any adverse effect.
     */
    /*
     * It may be possible that if two successive workers are unable to get some work
     * from the reserve and start signaling this fact to other workers that some
     * workers may find themselves repeatedly asked to place some work in the
     * reserve despite the fact they have just done so. This could be alleviated if
     * more restrictive synchronization mechanisms were used but I don't think these
     * would actually bring about any benefit.
     */
    private void reserveWasEmptied() {
        for (int i = 0; i < MAX_WORKERS; i++) {
            feedReserveRequested.set(i, 1);
        }
    }

    /**
     * Sub-routine used to signal to the local work reserve and the local operation
     * thread that an operation has had all its local assignments completed locally.
     *
     * @param op the operation whose assignments were entirely processed
     */
    void signalLocalOperationCompletion(@SuppressWarnings("rawtypes") GlbOperation op) {
        // We signal the local reserve instance that this operation has completed
        // locally.
        reserve.tasksWithWork.remove(op);

        // We unblock the operation thread that was waiting
        // This part is protected against concurrent accesses as a concurrent
        // GlbComputer#newOperation call may insert a new value into the map, causing
        // some troubles.
        synchronized (blockers) {
            blockers.get(op).unblock();
        }
    }

    /**
     * Main activity of an operation. This method is called when an operation
     * becomes available for the global load balancer. It consists of several steps:
     * <ol>
     * <li><em>Initialization phase</em> which prepares the GlbTask and other
     * members needed for later synchronization for this new operation
     * <li><em>Computation phase</em> during which this activity blocks on a
     * {@link java.util.concurrent.ForkJoinPool.ManagedBlocker} while workers that
     * are not bound to any activity perform the computation. When this operation
     * has been completed locally, this activity will be woken up to perform
     * work-stealing operations through its lifelines.
     * <li><em>Lifeline phase</em> in which phase this thread will asynchronously
     * signal the neighboring places that it needs work.
     * </ol>
     *
     * @param op the operation which is being launched on this host
     */
    void witnessActivity(@SuppressWarnings("rawtypes") GlbOperation op) {
        attemptToSpawnWorker();

        // As this activity is going to block, it should unblock any potential worker
        // that yielded its execution to allow this activity to run
        workerYieldLock.unblock();

        // Block until operation completes on the local host
        try {
            // Take the appropriate SemaphoreBlocker to be waken up by workers
            final OperationBlocker mb;
            synchronized (blockers) {
                mb = blockers.get(op);
            }

            if (mb.allowedToBlock()) {
                if (TRACE) {
                    System.err.println(here() + ": thread waiting on operation " + op);
                }
                ForkJoinPool.managedBlock(mb);
                if (TRACE) {
                    System.out.println(here() + ": resumed after waiting on " + op);
                }
            } else {
                // There is already another thread blocking on this semaphore.
                // This thread was a lifeline answer and can return safely
                if (TRACE) {
                    System.err.println(here() + ": thread already waiting on operation " + op);
                }
                return;
            }
        } catch (final InterruptedException e) {
            System.err.println(
                    "InterruptedException received in operation activity, this should not happen as the managed blocker implementation does not throw this error");
            e.printStackTrace();
        }
        // Establish lifelines before returning
        establishingLifelineOnRemoteHost(op);
    }

    /**
     * Main routine of a worker in the distributed collections library global load
     * balancer
     * <p>
     * In this load balancer, a worker is not bound to any single task. Instead, it
     * takes work from any available operation and continues running until it
     * completely runs out of work. Workers are spawned with an initial assignment.
     * The main steps of this procedure are:
     * <ol>
     * <li>Process a part of the work fragment the worker has, as defined by
     * {@link #granularity}
     * <li>Attempt to spawn a new parallel worker from the work presumably available
     * in the {@link #reserve}
     *
     * <li>Check for any load balancing operation is needed
     * <li>If other asynchronous activities are waiting, yield its execution to
     * allow them to execute NOT IMPLEMENTED YET
     * </ol>
     * These steps are repeated in a loop until the Assignment held by the worker is
     * completed. When the worker exits that loop, it attempts to get a new
     * Assignment from the {@link #reserve}. If successful, repeat the procedure
     * from step 1. If unsuccessful, the worker stops.
     *
     * @param id {@link Integer} used to identify workers. The type of this
     *           parameter may be changed later to be able to attach some runtime
     *           facilities to the worker specific to some operations. For example
     *           when dealing with operation which consist in placing the results
     *           into {@link Bag}, we may want to initialize a single unique List
     *           for each worker with this list being re-used for multiple
     *           Assignments on this operation.
     * @param a  initial assignment to be processed by this worker
     */
    private void worker(WorkerInfo worker, Assignment a) {
        logger.put(LOGKEY_WORKER, LOG_WORKER_STARTED, Long.toString(System.nanoTime()));

        try {
            for (;;) {
                worker.currentOperation = a.chooseOperationToProgress();
                while (a.process(granularity, worker, worker.currentOperation)) { // STEP 1: Work is done here

                    // STEP 2: Attempt to spawn a new worker from work present in the reserve
                    attemptToSpawnWorker();

                    // STEP 3: Load Balance operations
                    if (feedReserveRequested.get(worker.id) == 1) { // If feeding the reserve is requested
                        if (a.isSplittable(granularity)) {
                            a.splitIntoGlbTask();
                            feedReserveRequested.set(worker.id, 0);
                            worker.assignmentSplit++; // Log the action
                        } else {
                            worker.assignmentUnabledToSplit++; // Log
                        }
                    }

                    // STEP 4: Yield if need be
                    TimeoutBlocker l;
                    if (POOL.hasQueuedSubmissions() && (l = workerAvailableLocks.poll()) != null) {
                        l.reset();
                        logger.put(LOGKEY_WORKER, LOG_WORKER_YIELDING, Long.toString(System.nanoTime()));
                        ForkJoinPool.managedBlock(l);
                        logger.put(LOGKEY_WORKER, LOG_WORKER_RESUMED, Long.toString(System.nanoTime()));
                        workerAvailableLocks.add(l);
                    }

                    // STEP 5: Answer lifeline thieves if there are any and this host is capable of
                    // doing so
                    final LifelineToken steal = lifelineThieves.poll();
                    if (steal != null && steal.batchId >= currentBatch.get(steal.collection)) {
                        // Check if the target collection has some assignments left for the target
                        // collection

                        GlbTask g;
                        if ((g = reserve.allTasks.get(steal.collection)) != null && g.answerLifeline(steal)) {
                            logger.put(LOGKEY_WORKER, LOG_LIFELINE_ANSWERED, Long.toString(System.nanoTime()));
                        } else {
                            logger.put(LOGKEY_WORKER, LOG_LIFELINE_NOT_ANSWERED, Long.toString(System.nanoTime()));
                            lifelineThieves.add(steal);
                        }
                    }
                }
                // Assignment#process method returned false. This worker needs a new assignment.

                // Trying to obtain a new assignment determines if the worker continue or stop
                if ((a = reserve.getAssignment(worker)) == null) {
                    // The reserve returned null, this worker will stop

                    workerYieldLock.unblock(); // As this worker quits, any waiting worker can resume
                    logger.put(LOGKEY_WORKER, LOG_WORKER_STOPPED, Long.toString(System.nanoTime()));
                    return;
                } else {
                    // This worker was able to take an assignment from the reserve. It is not
                    // starting the for(;;) loop again.
                    worker.tookFromReserve++;
                }
            }
        } catch (final Throwable t) {
            System.err.println("Worker number " + worker.id + " on " + here() + " sufferred a " + t);
            t.printStackTrace();
        }
    }
}
