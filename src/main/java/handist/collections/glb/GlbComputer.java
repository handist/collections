package handist.collections.glb;

import static apgas.Constructs.*;
import static apgas.ExtendedConstructs.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinPool;

import apgas.impl.Finish;
import apgas.util.PlaceLocalObject;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Distributed object in charge of handling the GLB runtime and the work
 * stealing and between hosts.
 *
 * @author Patrick
 *
 */
class GlbComputer extends PlaceLocalObject {

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
	 * Array of GlbTask representing each distributed collection with operations in
	 * progress on this local place
	 */
	@SuppressWarnings("rawtypes")
	HashMap<GlbOperation, GlbTask> tasksWithWork;

	/**
	 * Constructor
	 * <p>
	 * Prepares the members of WorkReserve to receive the various GLB operations
	 */
	WorkReserve() {
	    allTasks = new HashMap<>();
	    tasksWithWork = new HashMap<>();

	    finishes = new HashMap<>();
	    blockers = new HashMap<>();
	}

	/**
	 * Checks the GlbTask of the current host to provide some work to a worker. The
	 * calling worker needs to provide a {@link TaskProgress} instance in which the
	 * work will be returned.
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

	    // The GlbTasl may already be present in tasksWithWork if another operation is
	    // being processed already. HashSet#add will keep the tasksWithWork set
	    // unchanged in this case.
	    tasksWithWork.put(op, toPlaceInAvailable);
	    return toPlaceInAvailable.newOperation(op);
	}
    }

    /** Singleton, local handle instance */
    private static GlbComputer computer = null;

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
    int currentWorkers;

    /**
     * Number of elements processed by workers in one gulp before they check the
     * runtime
     */
    volatile int granularity = 1;

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
	// MAX_WORKERS = Runtime.getRuntime().availableProcessors();
	MAX_WORKERS = 1; // For now, will be increased later
	reserve = new WorkReserve();
    }

    /**
     * Registers a new operation as available to workers and starts workers if they
     * were not already launched. The thread that called this method is then blocked
     * until the computation has terminated globally/locally ?
     *
     * @param op the operation newly available to process
     */
    @SuppressWarnings("rawtypes")
    void newOperation(final GlbOperation op) {
	// Put members needed for global termination management
	reserve.finishes.put(op, currentFinish());
	reserve.blockers.put(op, new SemaphoreBlocker());

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
     */
    void operationActivity(@SuppressWarnings("rawtypes") GlbOperation op) {
	// Spawn a worker (uncounted)
	synchronized (this) {
	    if (currentWorkers == 0) {
		currentWorkers = 1;
		uncountedAsyncAt(here(), () -> worker());
	    }
	}

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
     * completely runs out of work. The main steps in its main procedure are:
     * <ol>
     * <li>Take a fragment of work from the local reserve
     * <li>Process a part of this fragment
     * <li>Check for any load balancing operation needed (spawning a new worker/
     * placing some work back into the reserve)
     * <li>If other asynchronous activities are waiting, yield its execution to
     * allow them to execute
     * <li>When the fragment taken is completely consumed, try to take another one
     * from the local reserve and repeat
     * <li>If unsuccessful in taking some new work, stop
     * </ol>
     */
    private void worker() {
	Assignment a;
	int remainingWorkers = 0;
	for (;;) {
	    // Trying to obtain a new assignment determines if the worker continue or stop,
	    // it needs to be done in a synchronized block
	    synchronized (this) {
		if ((a = reserve.getAssignment()) == null) {
		    remainingWorkers = currentWorkers--;
		    break;
		}
	    }
	    try {
		while (a.process(granularity)) {
		    // TODO yield to other tasks, handle remote steals ...
		}
		// Assignment#process method returned false. This worker needs a new assignment.
	    } catch (final Throwable t) {
		t.printStackTrace();
	    }
	}

	if (remainingWorkers == 0) {
	    // Just to be sure, release all locks
	    for (final SemaphoreBlocker b : reserve.blockers.values()) {
		b.unblock();
	    }
	}
    }
}
