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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import handist.collections.LongRange;
import handist.collections.dist.DistCol;

/**
 * Implementation of GlbTask for the {@link DistCol} distributed collection.
 * This implementation relies on {@link LongRange} to describe assignments taken
 * up by workers.
 *
 * @author Patrick Finnerty
 *
 */
public class DistColGlbTask implements GlbTask {

    /**
     * Class describing the progress of the various operations taking place on a
     * range pertaining to the {@link handist.collections.dist.DistCol}
     *
     * @author Patrick Finnerty
     *
     */
    class DistColAssignment implements Assignment {

        /**
         * Progress of each operation in progress on this range
         * <p>
         * As operation are completed on this assignment, the mapping for this operation
         * will be removed.
         */
        @SuppressWarnings("rawtypes")
        Map<GlbOperation, Long> progress;

        /** Range of indices on which this assignment will operate */
        LongRange range;

        /**
         * Constructor
         *
         * Builds a new assignment with a dedicated LongRange instance
         *
         * @param lr range of entries on which the assignment is being created and on
         *           which the various operations will operate
         */
        DistColAssignment(LongRange lr) {
            range = lr;
            progress = new ConcurrentHashMap<>();
        }

        @SuppressWarnings("rawtypes")
        @Override
        public GlbOperation chooseOperationToProgress() {
            for (final Map.Entry<GlbOperation, Long> e : progress.entrySet()) {
                if (e.getValue() < range.to) { // This operation has work left, we pick it
                    return e.getKey();
                }
            }
            throw new RuntimeException("Could not obtain an operation with work in assignment " + this);
        }

        /**
         * Indicates if an assignment of {@link DistCol} can be split in two assignments
         * with work in both of them. For an assignment of {@link DistCol}, this method
         * will return {@code true} if the following two conditions are met:
         * <ol>
         * <li>The range of this assignment is greater than the provided parameter
         * <li>There is at least one operation in progress on this assignment which has
         * more than the provided parameter of instances left to process
         * </ol>
         */
        @Override
        public boolean isSplittable(int qtt) {
            // First condition, range greater than minimum
            if (range.size() <= qtt) {
                return false;
            }

            // Second condition, at least one operation has greater than minimum elements
            // left to process
            for (final Long operationProgress : progress.values()) {
                if (range.to - operationProgress >= qtt) {
                    return true;
                }
            }

            return false;
        }

        /**
         * Indicates if there are some operations which have not completed yet on this
         * assignment
         *
         * @return true if there are uncompleted operation on this assignment
         */
        boolean operationRemaining() {
            for (final Long l : progress.values()) {
                if (l < range.to) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Processes the specified amount of elements in a GlbOperation which is
         * available for this assignment.
         *
         * @param qtt number of elements to process
         * @param ws  service provided by the worker to the operation
         * @param op  GLB operation to progress in this assignment
         * @return true if there is some work remaining in the operation that was
         *         progressed, false if the operation that was chosen was completed on
         *         this fragment
         */
        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public boolean process(int qtt, WorkerService ws, GlbOperation op) {
            final long next = progress.get(op);

            long limit = next + qtt;
            boolean operationCompletedForThisAssignment = false;
            if (limit >= range.to) {
                // The operation selected will be completed on this assignment
                limit = range.to;
                operationCompletedForThisAssignment = true;
            }

            /*
             * I have getting some NPE thrown from the following call. This issue might have
             * been fixed but the try/catch will remain for a while just in case.
             */
            try {
                progress.put(op, limit);
            } catch (final NullPointerException e) {
                e.printStackTrace();
                System.err.println("Key was: " + op + " and value was " + limit);
                throw e;
            }

            // Computation loop is made on the following LongRange inside the "action"
            // carried by the GlbOperation.
            final LongRange lr = new LongRange(next, limit);
            op.operation.accept(lr, ws);

            // Signal the parent GlbTask that the operation has completed on this
            // assignment.
            if (operationCompletedForThisAssignment) {
                operationTerminatedOnAssignment(op);
                // Depending if there are other operations on this assignment, we
                // place it the appropriate collection of the parent DistColGlbTask

                // This part is protected using a read/write lock against newOperation.
                lockForWorkAssignmentSplittingAndNewOperation.readLock().lock();
                if (operationRemaining()) {
                    assignedAssignments.remove(this);
                    availableAssignments.add(this);
                } else {
                    assignedAssignments.remove(this);
                    completedAssignments.add(this);
                }
                lockForWorkAssignmentSplittingAndNewOperation.readLock().unlock();
            }

            return !operationCompletedForThisAssignment;
        }

        /**
         * Splits this assignment, creating a new one which is stored in the enclosing
         * {@link DistColGlbTask} immediately.
         * <p>
         * The current assignment will be split at the halfway point between the
         * operation with the lowest progress and the {@link #range} upper bound. This
         * ensures that there is work both in the assignment that is split away and the
         */
        @Override
        public void splitIntoGlbTask() {
            lockForWorkAssignmentSplittingAndNewOperation.readLock().lock();
            // First determine the splitting point
            long minimumProgress = Long.MAX_VALUE;
            for (final Long operationProgress : progress.values()) {
                minimumProgress = operationProgress < minimumProgress ? operationProgress : minimumProgress;
            }
            final long splittingPoint = range.to - ((range.to - minimumProgress) / 2);

            // Create a 'split' assignment
            final LongRange splitRange = new LongRange(splittingPoint, range.to);
            final LongRange thisRange = new LongRange(range.from, splittingPoint);
            final DistColAssignment split = new DistColAssignment(splitRange);
            range = thisRange;

            // Adjust the progress of both "this" and the created assignment
            for (@SuppressWarnings("rawtypes")
            final Map.Entry<GlbOperation, Long> progressEntry : progress.entrySet()) {
                final Long currentProgress = progressEntry.getValue();
                @SuppressWarnings("rawtypes")
                final GlbOperation op = progressEntry.getKey();
                if (currentProgress < splitRange.from) {
                    // The progress of this operation remains unchanged for this (it is within range
                    // of "thisRange")

                    // The start of the "splitRange" is set as the initial progress for the "split"
                    // assignment
                    split.progress.put(op, new Long(splittingPoint));

                    // There is an extra Assignment with work on this operation
                    // We increment the assignmentsLeftToProcess counter
                    assignmentsLeftToProcess.get(op).incrementAndGet();
                } else {
                    // The current progress is out of range for "thisRange", we set it to the upper
                    // bound: thisRange.to.
                    // This is not actually necessary but it will probably make things less
                    // confusing when debugging.
                    progress.put(op, thisRange.to);

                    // The current progress is placed in the "split" progress.
                    // Note that it is possible for "currentProgress" to be equal to
                    // "splitRange.to" if at the time this method is called the operation had
                    // already completed.
                    split.progress.put(op, currentProgress);

                    // Whether there was work or not, the number of assignments that need to
                    // complete
                    // for this operation remains unchanged. We do not need to increment the
                    // assignmentsLeftToProcess counter for this operation
                }
            }

            // Add the "splitAssignment" to the DistColGlbTask handling the assignment for
            // the underlying collection.
            // NOTE: The counter for the total number of assignments needs to be incremented
            // BEFORE the assignment is placed in the "available" queue.
            totalAssignments.incrementAndGet();
            availableAssignments.add(split);

            lockForWorkAssignmentSplittingAndNewOperation.readLock().unlock();
        }

        @Override
        public String toString() {
            return range.toString();
        }
    }

    /**
     * Contains the list of all the assignments that are being processed by a worker
     */
    ConcurrentLinkedQueue<DistColAssignment> assignedAssignments;

    /**
     * Map which associates the number of assignments left to process to each
     * operation in progress.
     * <p>
     * As workers complete operations of assignments, they will decrement the
     * matching atomic counter. When a worker completes the last assignment
     * available for an operation (i.e. the counter was decremented to 0), local
     * completion of this operation is reached. That worker will therefore trigger
     * the stealing process by releasing the operation thread which is currently
     * blocking on a {@link SemaphoreBlocker}.
     */
    @SuppressWarnings("rawtypes")
    Map<GlbOperation, AtomicInteger> assignmentsLeftToProcess;

    /** Contains the list of all the assignments that are available to workers */
    ConcurrentLinkedQueue<DistColAssignment> availableAssignments;

    /**
     * Contains all the assignments that have been completely processed by a worker.
     * <p>
     * The assignments in this collection have all of the current operations
     * completed.
     */
    ConcurrentLinkedQueue<DistColAssignment> completedAssignments;

    /**
     * This lock is used to maintain consistency of the total assignment counter and
     * the presence of {@link AtomicLong} in {@link DistColAssignment#progress}.
     * <p>
     * "Readers" are threads that perform the {@link #splitIntoGlbTask()} method
     * while "Writers" are threads that call the {@link #newOperation(GlbOperation)}
     * method. There can be many concurrent calls to the {@link #splitIntoGlbTask()}
     * method but not when a new operation becomes available.
     */
    ReadWriteLock lockForWorkAssignmentSplittingAndNewOperation;

    /**
     * Total number of assignments located on this place
     */
    AtomicInteger totalAssignments;

    /**
     * Constructor
     *
     * @param localHandle the local handle of the collection which is going to
     *                    undergo some operations under this class' supervision
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    DistColGlbTask(DistCol localHandle) {
        availableAssignments = new ConcurrentLinkedQueue<>();
        assignedAssignments = new ConcurrentLinkedQueue<>();
        completedAssignments = new ConcurrentLinkedQueue<>();
        assignmentsLeftToProcess = new HashMap<>();
        lockForWorkAssignmentSplittingAndNewOperation = new ReentrantReadWriteLock();

        // Initialize assignments with each LongRange of the local handle
        final Collection<LongRange> ranges = localHandle.getAllRanges();
        totalAssignments = new AtomicInteger(ranges.size());
        ranges.forEach((l) -> {
            final LongRange lr = l;
            final LongRange copyForAssignment = new LongRange(lr.from, lr.to);
            final DistColAssignment a = new DistColAssignment(copyForAssignment);
            availableAssignments.add(a);
        });
    }

    @Override
    public Assignment assignWorkToWorker() {
        lockForWorkAssignmentSplittingAndNewOperation.readLock().lock();
        final DistColAssignment a = availableAssignments.poll();
        if (a != null) {
            assignedAssignments.add(a);
        }
        lockForWorkAssignmentSplittingAndNewOperation.readLock().unlock();
        return a;
    }

    /**
     * Initializes the progress tracking in every assignment contained in this local
     * instance for the provided operation.
     * <p>
     * This method acquires the "WriteLock" of this instance to be protected against
     * calls to
     * <ul>
     * <li>{@link #assignWorkToWorker()}
     * <li>DistColAssignment method used to split assignment
     * </ul>
     *
     * @param op the new operation available for processing
     * @return true if some new work is available on the local host as a result of
     *         this new operation. A case where this method would return false is if
     *         there were no elements in the local handle of {@link DistCol}.
     */
    @Override
    public boolean newOperation(@SuppressWarnings("rawtypes") GlbOperation op) {
        lockForWorkAssignmentSplittingAndNewOperation.writeLock().lock();

        // We allocate an extra counter for completed assignments
        // This counter is used in #operationTerminatedOnAssignment(GlbOperation)
        // to check if all assignments of the DistColGlbTask have been performed
        assignmentsLeftToProcess.put(op, new AtomicInteger(totalAssignments.get()));

        // Add a progress tracker for each assignment contained locally
        boolean toReturn = false;
        for (final DistColAssignment a : availableAssignments) {
            a.progress.put(op, a.range.from);
            toReturn = true;
        }

        for (final DistColAssignment a : assignedAssignments) {
            a.progress.put(op, a.range.from);
            toReturn = true;
        }

        for (final DistColAssignment a : completedAssignments) {
            a.progress.put(op, a.range.from);
            toReturn = true;
        }

        // The formerly completed assignments now have work in them
        availableAssignments.addAll(completedAssignments);
        completedAssignments.clear();

        lockForWorkAssignmentSplittingAndNewOperation.writeLock().unlock();

        return toReturn;
    }

    /**
     * Signals that the specified operation has been completed for one of the
     * assignments. This method is called by a worker thread from method
     * {@link DistColAssignment#process(int)} as it was processing the assignment.
     *
     * @param op operation on which an assignment has completed
     */
    void operationTerminatedOnAssignment(@SuppressWarnings("rawtypes") GlbOperation op) {
        final AtomicInteger ai = assignmentsLeftToProcess.get(op);
        final int completedAssignments = ai.decrementAndGet();

        if (completedAssignments == 0) {
            final GlbComputer glb = GlbComputer.getComputer();

            // This was the last assignment for this operation
            // We unblock the operation thread that was waiting
            glb.reserve.blockers.get(op).unblock();

            // We also signal the local reserve instance that this operation has completed
            // locally.
            glb.reserve.tasksWithWork.remove(op);
        }
    }
}
