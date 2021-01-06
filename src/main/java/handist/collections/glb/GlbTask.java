package handist.collections.glb;

/**
 * Interface of the object which manages the assignments of a distributed
 * collection on a local host.
 *
 * @author Patrick Finnerty
 *
 */
public interface GlbTask {

    /**
     * Assigns some work, i.e. a portion of the underlying collection which needs to
     * undergo an operation to the asking worker
     *
     * @return an assignment which the worker will process, or null if impossible to
     *         assign some work at the time this method was called
     */
    Assignment assignWorkToWorker();

    /**
     * Signals this GlbTask that a new operation is available for computation on the
     * underlying distributed collection.
     *
     * @param op the new operation available to workers
     * @return true if initializing the operation resulted in any work being made
     *         available to workers, false otherwise
     */
    boolean newOperation(@SuppressWarnings("rawtypes") GlbOperation op);
}
