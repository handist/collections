package handist.collections.glb;

/**
 * Portion of work to assign to a worker.
 * <p>
 * The interface represents a portion of a distributed collection on which an
 * operation needs to be performed. Assignments are assigned to workers or
 * otherwise kept in reserve of work inside their
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

    /**
     * Indicates if this assignment can be split into 2 new assignments. An
     * assignment is considered splittable if splitting the assignment will result
     * in some work being available for other workers, and there is at least one
     * operation for this assignment which contains more work than the integer
     * quantity specified as parameter.
     *
     * @return {@code true} if splitting this assignment will result in more work
     *         being available for other workers, {@code false otherwise}
     */
    boolean isSplittable(int qtt);

    /**
     * Splits this assignment and places the created Assignment into the
     * {@link GlbTask} which is handling the assignments of the distributed
     * collection on which this assignment operates.
     */
    void splitIntoGlbTask();
}
