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
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import apgas.Place;
import apgas.impl.Finish;
import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.glb.Config.LifelineAnswerMode;
import handist.collections.glb.GlbComputer.LifelineToken;

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
    static class DistColAssignment implements Assignment {

        /** Serial Version UID */
        private static final long serialVersionUID = 5397031649035798704L;

        /**
         * This member avoids re-computing the priority of this assignment repeatedly.
         * Only when an operation on this assignment completes that this member is
         * updated.
         */
        private int currentPriority;

        /**
         * {@link DistColGlbTask} currently handling this assignment. This member is not
         * serialized as it will need to be set to an existing object when the
         * assignment is received on the remote host.
         */
        transient DistColGlbTask parent;

        /**
         * Progress of each operation in progress on this range
         * <p>
         * As operation are completed on this assignment, the mapping for this operation
         * will be removed.
         */
        @SuppressWarnings("rawtypes")
        final ConcurrentSkipListMap<GlbOperation, Long> progress;

        /** Range of indices on which this assignment will operate */
        LongRange range;

        /**
         * Constructor
         *
         * Builds a new assignment with a dedicated LongRange instance
         *
         * @param lr range of entries on which the assignment is being created and on
         *           which the various operations will operate
         * @param p  parent {@link DistColGlbTask} in charge of this instance
         */
        DistColAssignment(LongRange lr, DistColGlbTask p) {
            range = lr;
            progress = new ConcurrentSkipListMap<>();
            parent = p;
            currentPriority = Integer.MAX_VALUE;
        }

        /*
         * As the progress member is a concurrenSkipList which sorts the entries using
         * the priority ordering of GlbOperation, the first entry corresponds to the
         * operation on this assignment with the highest priority.
         *
         * Of course, if there is a single operation taking place on the underlying
         * collection, the first entry will also be the only one.
         */
        @SuppressWarnings("rawtypes")
        @Override
        public GlbOperation chooseOperationToProgress() {
            return progress.firstKey();
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

        @Override
        public int priority() {
            return currentPriority;
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

            progress.put(op, limit);

            // Computation loop is made on the following LongRange inside the "action"
            // carried by the GlbOperation.
            final LongRange lr = new LongRange(next, limit);
            op.operation.accept(lr, ws);

            // Signal the parent GlbTask that the operation has completed on this
            // assignment.
            if (operationCompletedForThisAssignment) {
                parent.operationTerminatedOnAssignment(op);
                // We remove the tracker for the current operation
                progress.remove(op);

                // Depending if there are other operations on this assignment, we
                // place it the appropriate collection of the parent DistColGlbTask

                // This part is protected using a read/write lock against newOperation.
                parent.lockForWorkAssignmentSplittingAndNewOperation.readLock().lock();
                if (!progress.isEmpty()) {
                    // There are other operations left
                    // Update the priority before placing it back into the availableAssignments
                    // collection
                    updatePriority();
                    parent.assignedAssignments.remove(this);
                    parent.availableAssignments.add(this);
                } else {
                    // No other operations left
                    parent.assignedAssignments.remove(this);
                    parent.completedAssignments.add(this);
                }
                parent.lockForWorkAssignmentSplittingAndNewOperation.readLock().unlock();
            }
            return !operationCompletedForThisAssignment;
        }

        /**
         * Allows to set the DistColGlbTask in charge of this assignment. This is used
         * when receiving assignments as part of a lifeline answer to make the
         * assignments used to correct objects accessed through its parent member.
         *
         * @param p the new value for member parent
         */
        void setParent(DistColGlbTask p) {
            parent = p;
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
            parent.lockForWorkAssignmentSplittingAndNewOperation.readLock().lock();
            // First determine the splitting point
            long minimumProgress = Long.MAX_VALUE;
            for (final Long operationProgress : progress.values()) {
                minimumProgress = operationProgress < minimumProgress ? operationProgress : minimumProgress;
            }
            final long splittingPoint = range.to - ((range.to - minimumProgress) / 2);

            // Create a 'split' assignment
            final LongRange splitRange = new LongRange(splittingPoint, range.to);
            final LongRange thisRange = new LongRange(range.from, splittingPoint);
            final DistColAssignment split = new DistColAssignment(splitRange, parent);
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
                    parent.assignmentsLeftToProcess.get(op).incrementAndGet();
                } else {
                    // The current progress is out of range for "thisRange", we remove the tracker
                    // as if it had completed for this assignment (in effect, this has the same
                    // consequences as if the operation
                    progress.remove(op);

                    // The current progress is placed in the "split" progress.
                    // Note that it is not possible for "currentProgress" to be equal to
                    // "splitRange.to" as the Long trackers kept in member progress are removed when
                    // an operation is completed
                    split.progress.put(op, currentProgress);

                    // Whether there was work or not, the number of assignments that need to
                    // complete
                    // for this operation remains unchanged. We do not need to increment the
                    // assignmentsLeftToProcess counter for this operation
                }
            }

            // The progress for the operations may have changed for both assignments, we
            // refresh their priority
            updatePriority();
            split.updatePriority();

            // Add the "splitAssignment" to the DistColGlbTask handling the assignment for
            // the underlying collection.
            // NOTE: The counter for the total number of assignments needs to be incremented
            // BEFORE the assignment is placed in the "available" queue.
            parent.totalAssignments.incrementAndGet();
            parent.availableAssignments.add(split);

            parent.lockForWorkAssignmentSplittingAndNewOperation.readLock().unlock();
        }

        @Override
        public String toString() {
            return range.toString();
        }

        /**
         * Updates the priority level of this Assignment.
         *
         * This method needs to be called when a new operation is made available to this
         * assignment or when an operation contained by this assignment completes
         */
        private void updatePriority() {
            @SuppressWarnings("rawtypes")
            final Map.Entry<GlbOperation, Long> entry = progress.firstEntry();
            currentPriority = entry == null ? Integer.MAX_VALUE : entry.getKey().priority;
        }
    }

    /**
     * Answer mode used to make lifeline answers
     */
    private static final LifelineAnswerMode answerMode = Config.getLifelineSerializationMode();

    /** Serial Version UID */
    private static final long serialVersionUID = -792674800264517475L;

    /**
     * Upper bound on the number of assignments which can be transferred to a remote
     * thief.
     */
    private static final int MAX_NUMBER_STOLEN_ASSIGNMENTS = 10;

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
     * blocking on a {@link OperationBlocker}.
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
     * method but not when a new operation becomes available and a number of
     * modifications to several members of this class need to be made atomically to
     * preserve consistency.
     */
    transient final ReadWriteLock lockForWorkAssignmentSplittingAndNewOperation;

    /**
     * Total number of assignments located on this place
     */
    AtomicInteger totalAssignments;

    /**
     * Underlying collection on which the assignments operate
     */
    @SuppressWarnings("rawtypes")
    private final DistCol collection;

    /**
     * Constructor
     *
     * @param localHandle the local handle of the collection which is going to
     *                    undergo some operations under this class' supervision
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    DistColGlbTask(DistCol localHandle) {
        collection = localHandle;
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
            final DistColAssignment a = new DistColAssignment(copyForAssignment, this);
            availableAssignments.add(a);
        });
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean answerLifeline(final LifelineToken token) {
        final Place thief = token.place;

        // START OF THE R/W LOCK PROTECTION
        lockForWorkAssignmentSplittingAndNewOperation.readLock().lock();
        // Obtain some Assignments from the work reserve
        // TODO we may need a better way to decide how much work a thief should be able
        // to take.
        final ArrayList<Assignment> stolen = new ArrayList<>();
        DistColAssignment a;
        while (stolen.size() < MAX_NUMBER_STOLEN_ASSIGNMENTS && (a = availableAssignments.poll()) != null) {
            stolen.add(a);
        }
        // Decrement the total number of assignments contained locally
        totalAssignments.getAndAdd(-stolen.size());
        // END OF THE R/W LOCK PROTECTION
        lockForWorkAssignmentSplittingAndNewOperation.readLock().unlock();

        if (stolen.isEmpty()) {
            // If no assignment could be taken, there is nothing more to do and the method
            // return false here
            return false;
        }

        // From here onward, we know that some assignments were taken from the reserve.
        // We flip the place to "here" as the token will be used to indicate to the
        // thief that this place is the one making the answer.
        token.place = here();

        /*
         * Detect which operations are contained in the assignments stolen to determine
         * under which finish we need to make the lifeline answer. By the same occasion,
         * count how many assignments of each operation were taken away. This allows us
         * to decrement the number of assignments left to complete after the assignment
         * transfer has completed
         */
        final HashMap<GlbOperation, Integer> numbers = new HashMap<>();

        for (final Assignment s : stolen) {
            final DistColAssignment assignment = (DistColAssignment) s;
            final long upperBound = assignment.range.to;
            assignment.progress.entrySet().forEach((entry) -> {
                final Long progress = entry.getValue();
                final GlbOperation op = entry.getKey();
                if (progress < upperBound) { // If the operation has work left
                    final Integer v = numbers.computeIfAbsent(op, k -> new Integer(0));
                    numbers.put(op, new Integer(v + 1)); // Increment the counter
                }
            });
        }

        // Prepare the array of enclosing finishes
        final Set<GlbOperation> operations = numbers.keySet();
        final HashMap<GlbOperation, Finish> finishes = new HashMap<>();
        final Finish[] finishArray = new Finish[operations.size()];
        final GlbComputer glb = GlbComputer.getComputer();
        int fidx = 0;
        for (final GlbOperation op : operations) {
            final Finish f = glb.finishes.get(op);
            finishes.put(op, f);
            finishArray[fidx++] = f;
        }

        /*
         * Make the asynchronous answer to the target place. We need serialization of a
         * certain number of elements and the assignments. Then we need to
         * asynchronously accept these instances and merge them into the local bag and
         * check whether a new operation thread is needed.
         */
        // Initialize the one-sided move manager
        final CustomOneSidedMoveManager m = new CustomOneSidedMoveManager(thief);
        // Submit all the elements of the collection that need to be moved
        for (final Assignment s : stolen) {
            final DistColAssignment assignment = (DistColAssignment) s;
            collection.moveRangeAtSync(assignment.range, thief, m);
        }

        switch (answerMode) {
        case MPI:
            try {
                m.asyncSendAndDoWithMPI(
                        () -> GlbComputer.getComputer().lifelineAnswer(token, stolen, numbers, finishes), finishArray);
            } catch (final Exception e) {
                System.err.println("Error while trying to transfer work");
                e.printStackTrace();
            }
            break;
        case KRYO:
        default:
            try {
                m.asyncSendAndDoNoMPI(() -> GlbComputer.getComputer().lifelineAnswer(token, stolen, numbers, finishes),
                        finishArray);
            } catch (final IOException e) {
                System.err.println("Error while trying to transfer work");
                e.printStackTrace();
            }
        }
        // The entries for the distributed collection have been transferred, as well as
        // all the assignments.

        // We decrement the numbers of remaining assignments to process as if they had
        // been completed locally.
        for (final Map.Entry<GlbOperation, Integer> entry : numbers.entrySet()) {
            final GlbOperation operation = entry.getKey();
            final Integer removedAssignments = entry.getValue();

            final AtomicInteger remainder = assignmentsLeftToProcess.get(operation);
            if (remainder.addAndGet(-removedAssignments) == 0) { // Decrements the counter
                // All assignments for this operation have completed locally
                GlbComputer.getComputer().signalLocalOperationCompletion(operation);
            }
        }

        return true;
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
     * Merges the given assignments into this GlbTask. This method is called by a
     * lifeline answer after the instances on which the assignment operate have been
     * integrated into the underlying {@link DistCol}.
     *
     * @param quantities  the number of assignment which have work for each glb
     *                    operation entered as a key in this map
     * @param assignments the assignments that were stolen
     */
    @Override
    @SuppressWarnings("rawtypes")
    public void mergeAssignments(HashMap<GlbOperation, Integer> quantities, ArrayList<Assignment> assignments) {
        // As the first part and before placing assignments into the queues, we
        // increment the counters for the number of assignments left to process for each
        // operation. This can be done without any particular protections.
        for (final Map.Entry<GlbOperation, Integer> entry : quantities.entrySet()) {
            final AtomicInteger i = assignmentsLeftToProcess.get(entry.getKey());
            assertNotNull(i);
            i.addAndGet(entry.getValue());
        }

        // We need to increment the counter for the number of assignments contained
        // locally, as well as placing all the assignment in the "availableAssignments"
        // queue. This needs to be done under STRONG protection: we use the writeLock
        lockForWorkAssignmentSplittingAndNewOperation.writeLock().lock();

        totalAssignments.addAndGet(assignments.size()); // Increment counter for the total number of assignments handled
                                                        // by this instance

        // All all assignments to the "availableAssignments" collection
        for (final Assignment a : assignments) {
            final DistColAssignment dca = (DistColAssignment) a; // Cast to the proper type
            dca.setParent(this); // From now on, "this" DistColGlbTask is handling the assignment
            availableAssignments.add(dca);
        }
        // The critical step has ended, we release the writeLock
        lockForWorkAssignmentSplittingAndNewOperation.writeLock().unlock();
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

        final int expected = totalAssignments.get();
        int prepared = 0;
        // Add a progress tracker for each assignment contained locally
        boolean toReturn = false;
        for (final DistColAssignment a : availableAssignments) {
            a.progress.put(op, a.range.from);
            a.updatePriority();
            toReturn = true;
            prepared++;
        }

        for (final DistColAssignment a : assignedAssignments) {
            a.progress.put(op, a.range.from);
            a.updatePriority();
            toReturn = true;
            prepared++;
        }

        for (final DistColAssignment a : completedAssignments) {
            a.progress.put(op, a.range.from);
            a.updatePriority();
            toReturn = true;
            prepared++;
        }
        assertEquals(expected, prepared); // The number of assignments prepared should be the same as the number of
                                          // assignments known to be held by this instance

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
            GlbComputer.getComputer().signalLocalOperationCompletion(op);
        }
    }
}
