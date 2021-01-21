package handist.collections.glb;

import static apgas.Constructs.*;
import static apgas.ExtendedConstructs.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicIntegerArray;

import apgas.impl.Finish;
import apgas.util.PlaceLocalObject;
import handist.collections.Bag;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Distributed object in charge of handling the GLB runtime and the work
 * stealing and between hosts.
 *
 * @author Patrick Finnerty
 *
 */
class GlbComputer extends PlaceLocalObject {

    /**
     * Class containing information and logging facilities of an individual worker.
     * As part of the initialization process for the GLB, an instance of this class
     * is prepared for each concurrent worker that may run on the host.
     *
     * @author Patrick Finnerty
     *
     */
    final static class WorkerInfo implements WorkerService {

	/**
	 * In some operations, an object may need to remain bound to a single worker and
	 * used throughout the processing of the operation. In such a case, this objects
	 * is kept in this collection.
	 */
	private final Map<Object, Object> workerBoundObjects;

	/** Time stamp of the last change in this worker's state */
	private long lastStamp;

	/**
	 * Time (in nanoseconds) spent working by this worker. The time spent yielding
	 * is not included.
	 */
	public long timeWorking;

	/** Time spent yielding to other tasks (not counted in {@link #timeWorking}) */
	public long timeYielding;

	/** Unique integer identifier */
	final int id;

	/**
	 * Counts the number of times this worker was spawned
	 */
	int workerSpawned;

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

	/**
	 * Counts the number of times this worker successfully takes an assignment from
	 * the reserve for itself
	 */
	int tookFromReserve;

	/**
	 * Signals that this worker started working
	 */
	void workerStarted() {
	    workerSpawned++;
	    lastStamp = System.nanoTime();
	}

	/**
	 * Signals that this worker which was working has now started yielding to other
	 * activities of the APGAS runtime.
	 */
	void workerYielding() {
	    final long stamp = System.nanoTime();

	    timeWorking += stamp - lastStamp;
	    lastStamp = stamp;
	}

	/**
	 * Signals that this worker which was yielding has now resumed working.
	 */
	void workerResumed() {
	    final long stamp = System.nanoTime();

	    timeYielding += stamp - lastStamp;
	    lastStamp = stamp;
	}

	/**
	 * Signals that this worker which was working has stopped.
	 */
	void workerStopped() {
	    final long stamp = System.nanoTime();
	    timeWorking += stamp - lastStamp;
	}

	@Override
	public void attachOperationObject(Object key, Object o) {
	    workerBoundObjects.put(key, o);
	}

	@Override
	public Object retrieveOperationObject(Object key) {
	    return workerBoundObjects.get(key);
	}

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
	 * Map from distributed collection to their respective GlbTask instance. This
	 * map is used to keep track of distributed collections that may already have an
	 * operation in progress. If that is the case and an additional operation comes
	 * along, this operation will need to be added to the existing {@link GlbTask}
	 * of this distributed collection.
	 */
	Map<Object, GlbTask> allTasks;

	/**
	 * Map associating every GlbOperation with the semaphore used to block the
	 * thread representing the presence of work for this operation.
	 */
	@SuppressWarnings("rawtypes")
	Map<GlbOperation, SemaphoreBlocker> blockers;

	/**
	 * Map associating every GlbOperation with the finish instance in which they are
	 * being executed
	 */
	@SuppressWarnings("rawtypes")
	Map<GlbOperation, Finish> finishes;

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
	ConcurrentHashMap<GlbOperation, GlbTask> tasksWithWork;

	/**
	 * Constructor
	 * <p>
	 * Prepares the members of WorkReserve to receive the various GLB operations
	 */
	WorkReserve() {
	    allTasks = new HashMap<>();
	    tasksWithWork = new ConcurrentHashMap<>();

	    finishes = new HashMap<>();
	    blockers = new HashMap<>();
	}

	/**
	 * Checks the GlbTask of the current host to provide some work to a worker.
	 * <p>
	 * If at the time this method is called no work could be selected, returns null
	 * instead.
	 *
	 * @return the provided instance with updates, or null if no work could be
	 *         obtained
	 */
	Assignment getAssignment() {
	    for (final GlbTask t : tasksWithWork.values()) {
		Assignment a;
		if ((a = t.assignWorkToWorker()) != null) {
		    return a;
		}
	    }
	    return null;
	}

	/**
	 * Registers a new operation in the local reserve
	 *
	 * @param op operation newly available to workers
	 * @return true if some work was actually made available to workers as a result
	 *         of this operation
	 */
	@SuppressWarnings("rawtypes")
	public synchronized boolean newOperation(GlbOperation op) {
	    // There is a GlbTask instance for the underlying collection (either created or
	    // retrieved)
	    GlbTask toPlaceInAvailable;
	    if (!allTasks.containsKey(op.collection)) {
		// We need to create a new GlbTask for the target collection
		toPlaceInAvailable = (GlbTask) op.initializerOfGlbTask.get();
		allTasks.put(op.collection, toPlaceInAvailable);
	    } else {
		// We "add" the operation to the existing instance
		toPlaceInAvailable = allTasks.get(op.collection);
	    }

	    // The GlbTask may already be present in tasksWithWork if another operation is
	    // being processed already. HashSet#add will keep the tasksWithWork set
	    // unchanged in this case.
	    tasksWithWork.put(op, toPlaceInAvailable);
	    return toPlaceInAvailable.newOperation(op);
	}
    }

    /** Singleton, local handle instance */
    private static GlbComputer computer = null;

    /**
     * Array containing all the workers initialized on the host Contrary to
     * {@link #idleWorkers}, the contents of this array never change. This array is
     * used to access all the workers irrespective of if they are running or idle.
     * <p>
     * One particular case we need to access all workers is when an operation needs
     * to attach an object to each worker which is needed when this operation is
     * processed by a the workers.
     */
    private final WorkerInfo[] workers;

    /**
     * Concurrent linked list which contains the inactive workers that may run on
     * the host if they are given an assignment.
     */
    private final ConcurrentLinkedDeque<WorkerInfo> idleWorkers;

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
     * Method returning the local singleton for GlbComputer
     *
     * @return GlbComputer local handle
     */
    static GlbComputer getComputer() {
	if (computer == null) {
	    computer = PlaceLocalObject.make(TeamedPlaceGroup.getWorld().places(), () -> {
		return new GlbComputer();
	    });
	}
	return computer;
    }

    /**
     * Number of concurrent workers running
     */
//    int currentWorkers;

    /**
     * Number of elements processed by workers in one gulp before they check the
     * runtime
     */
    volatile int granularity = 6;

    /**
     * Maximum number of workers that can concurrently run on the local host
     */
    final int MAX_WORKERS;

    /**
     * Instance in which the workers on a host take / place work back into
     */
    WorkReserve reserve;

    /**
     * Private constructor
     * <p>
     * GlbComputer is a global object that follows a singleton design pattern. This
     * constructor is made private to protect this property.
     */
    private GlbComputer() {
	// Set the maximum number of concurrent workers on this host
	MAX_WORKERS = Runtime.getRuntime().availableProcessors();
	workers = new WorkerInfo[MAX_WORKERS];

	// Prepare the worker Ids
	idleWorkers = new ConcurrentLinkedDeque<>();
	for (int i = 0; i < MAX_WORKERS; i++) {
	    final WorkerInfo w = new WorkerInfo(i);
	    idleWorkers.add(w);
	    workers[i] = w;
	}
	// Prepare the atomic array used as flag to signal to workers that the #reserve
	// needs to be fed
	feedReserveRequested = new AtomicIntegerArray(MAX_WORKERS);

	// Initialize the reserve of assignments for this host
	reserve = new WorkReserve();
    }

    /**
     * Registers a new operation as available to workers and starts workers if they
     * were not already launched. The thread that called this method is then blocked
     * until the computation has terminated globally/locally ?
     *
     * @param op the operation newly available to process
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    void newOperation(GlbOperation op) {
	// Put members needed for global termination management
	reserve.finishes.put(op, currentFinish());
	reserve.blockers.put(op, new SemaphoreBlocker());

	// Prepare every worker with the special initialization (if needed)
	if (op.workerInit != null) {
	    for (final WorkerInfo wi : workers) {
		op.workerInit.accept(wi);
	    }
	}

	// Prepare the GlbTask of the relevant distributed collection
	final boolean localWorkCreated = reserve.newOperation(op);

	// If there was some work created as a result, launch a worker / block until
	// completion
	// There can be cases where a local handle does not hold any element, in which
	// case we simply let this thread return.
	if (localWorkCreated) {
	    // Launch workers / block until local termination
	    operationActivity(op);
	} else {
	    // TODO Go on to work stealing directly.
	    // Careful with the activity actually in charge of it
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
    void operationActivity(@SuppressWarnings("rawtypes") GlbOperation op) {
	attemptToSpawnWorker();

	// Block until operation completes
	try {
	    // Take the appropriate SemaphoreBlocker to be waken up by workers
	    ForkJoinPool.managedBlock(reserve.blockers.get(op));
	} catch (final InterruptedException e) {
	    System.err.println(
		    "InterruptedException received in operation activity, this should not happen as the managed blocker implementation does not throw this error");
	    e.printStackTrace();
	}

	// The operation has completed. Moving on to establishing lifelines.
	// TODO
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
	worker.workerStarted();
	try {
	    for (;;) {
		while (a.process(granularity, worker)) { // STEP 1: Work is done here

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
		}
		// Assignment#process method returned false. This worker needs a new assignment.

		// Trying to obtain a new assignment determines if the worker continue or stop
		if ((a = reserve.getAssignment()) == null) {
		    // The reserve returned null, this worker will stop
		    worker.workerStopped();
		    idleWorkers.add(worker);
		    break;
		} else {
		    worker.tookFromReserve++;
		}
	    }
	} catch (final Throwable t) {
	    System.err.println("Worker number " + worker.id + " sufferred a " + t);
	    t.printStackTrace();
	}
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
	    final Assignment forSpawn = reserve.getAssignment();
	    if (forSpawn != null) {
		uncountedAsyncAt(here(), () -> worker(worker, forSpawn));
	    } else {
		// Work could not be taken from the reserve

		// We place the workerInfo back into the #workers collection so that it gets
		// another chance to be spawned later
		idleWorkers.add(worker);

		// We signal all the workers that the #reserve is empty and that every worker
		// needs to place some work back into the reserve
		reserveWasEmptied();
	    }
	}
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
}
