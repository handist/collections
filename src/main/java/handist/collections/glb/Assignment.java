package handist.collections.glb;

/**
 * Portion of work to assign to a worker.
 * <p>
 * The interface {@link Assignment} represents a portion of a distributed
 * collection on which an operation needs to be performed. Assignments are
 * assigned to workers or otherwise kept in reserve of work inside their
 *
 * @author Patrick Finnerty
 *
 */
interface Assignment {

    /**
     * Process a certain amount of an assignment on an operation available for the
     * current assignment. Then returns true if there remains some work for this
     * assignment that was progressed in this call, false otherwise.
     *
     * @param qtt amount of work to process
     * @return true if there is some work available for the operation that was
     *         progressed
     */
    boolean process(int qtt);
}
