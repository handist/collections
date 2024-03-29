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
import static handist.collections.glb.GlobalLoadBalancer.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

import apgas.Place;
import apgas.impl.Finish;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.DistChunkedList;
import handist.collections.glb.Config.LifelineAnswerMode;
import handist.collections.glb.GlbComputer.LifelineToken;

/**
 * Implementation of GlbTask for the {@link DistChunkedList} distributed
 * collection. This implementation relies on {@link LongRange} to describe
 * assignments taken up by workers.
 *
 * @author Patrick Finnerty
 *
 */
public class DistColGlbTask implements GlbTask {

    /**
     * Class describing the progress of the various operations taking place on a
     * range pertaining to the {@link DistChunkedList}
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
        final ConcurrentSkipListMap<GlbOperation, Progress> progress;

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
         * Indicates if an assignment of {@link DistChunkedList} can be split in two
         * assignments with work in both of them. For an assignment of
         * {@link DistChunkedList}, this method will return {@code true} if the
         * following two conditions are met:
         * <ol>
         * <li>The range of this assignment is greater than the provided parameter
         * <li>There is at least one operation in progress on this assignment which has
         * more than the provided parameter of instances left to process
         * </ol>
         */
        @Override
        public boolean isSplittable(int qtt) {
            // First condition to meet: range greater than minimum
            if (range.size() <= qtt) {
                return false;
            }

            // Second condition to meet: at least one operation has greater than the
            // "minimum" elements left to process
            for (final Progress operationProgress : progress.values()) {
                if (range.to - operationProgress.next >= qtt) {
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
            final Progress next = progress.get(op);

            long limit = next.next + qtt;
            boolean operationCompletedForThisAssignment = false;
            if (limit >= range.to) {
                // The operation selected will be completed on this assignment
                limit = range.to;
                operationCompletedForThisAssignment = true;
            }

            // Computation loop is made on the following LongRange inside the "action"
            // carried by the GlbOperation.
            final DistColLambda lambda = (DistColLambda) op.operation;
            lambda.process(parent.collection.getChunk(range), next.next, limit, ws);
            next.next = limit;

            // Signal the parent GlbTask that the operation has completed on this
            // assignment.
            if (operationCompletedForThisAssignment) {
                parent.operationTerminatedOnAssignment(op);
                // We remove the tracker for the current operation
                progress.remove(op);

                // If there are other operations contained in this assignment we place it back
                // into the queue after updating its "priority".
                if (!progress.isEmpty()) {
                    updatePriority();
                    parent.availableAssignments.add(this);
                }
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
            // First determine the splitting point
            long minimumProgress = Long.MAX_VALUE;
            for (final Progress operationProgress : progress.values()) {
                minimumProgress = operationProgress.next < minimumProgress ? operationProgress.next : minimumProgress;
            }
            final long splittingPoint = range.to - ((range.to - minimumProgress) / 2);

            // Create a 'split' assignment
            final LongRange splitRange = new LongRange(splittingPoint, range.to);
            final LongRange thisRange = new LongRange(range.from, splittingPoint);
            final DistColAssignment split = new DistColAssignment(splitRange, parent);
            range = thisRange;

            // Adjust the progress of both "this" and the created assignment
            for (@SuppressWarnings("rawtypes")
            final Map.Entry<GlbOperation, Progress> progressEntry : progress.entrySet()) {
                final Progress currentProgress = progressEntry.getValue();
                @SuppressWarnings("rawtypes")
                final GlbOperation op = progressEntry.getKey();
                if (currentProgress.next < splitRange.from) {
                    // The progress of this operation remains unchanged for this (it is within range
                    // of "thisRange")

                    // The start of the "splitRange" is set as the initial progress for the "split"
                    // assignment
                    split.progress.put(op, new Progress(splittingPoint));

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
            parent.availableAssignments.add(split);
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
            final Map.Entry<GlbOperation, Progress> entry = progress.firstEntry();
            currentPriority = entry == null ? Integer.MAX_VALUE : entry.getKey().priority;
        }
    }

    /**
     * Interface used to avoid packing and unpacking of types in the method called
     * by workers.
     *
     * @param <T> type of the entries on which the closure operates
     */
    static interface DistColLambda<T> extends Serializable {
        /**
         * Applies some closure on a range of entries
         *
         * @param chunk      the actual part of the
         * @param startIndex index of the first entry on which this closure should
         *                   operate
         * @param endIndex   index of the entry on which this closure should stop
         *                   (exclusive bound)
         * @param ws         context to retrieve worker-specific information necessary
         *                   for the computation
         */
        public void process(RangedList<T> chunk, long startIndex, long endIndex, WorkerService ws);
    }

    /**
     * Class used to avoid packing and unpacking {@link Long} and {@code long} when
     * tracking the progress of operations in member
     * {@link DistColAssignment#progress}
     */
    final static class Progress implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = -5134838227313756599L;
        /**
         * Index of the next entry to process for the operation tracked by this instance
         */
        long next;

        /**
         * Private constructor to avoid creation from outside the library
         *
         * @param initialValue
         */
        private Progress(long initialValue) {
            next = initialValue;
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
    HashMap<GlbOperation, AtomicInteger> assignmentsLeftToProcess;

    /** Contains the list of all the assignments that are available to workers */
    ConcurrentLinkedQueue<DistColAssignment> availableAssignments;

    /**
     * Underlying collection on which the assignments operate
     */
    @SuppressWarnings("rawtypes")
    private final DistChunkedList collection;

    /**
     * Constructor
     *
     * @param localHandle the local handle of the collection which is going to
     *                    undergo some operations under this class' supervision
     */
    DistColGlbTask(DistChunkedList<?> localHandle) {
        collection = localHandle;
        availableAssignments = new ConcurrentLinkedQueue<>();
        assignmentsLeftToProcess = new HashMap<>();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean answerLifeline(final LifelineToken token) {
        final Place thief = token.place;

        // Obtain some Assignments from the work reserve
        // TODO we may need a better way to decide how much work a thief should be able
        // to take.
        final ArrayList<Assignment> stolen = new ArrayList<>();
        DistColAssignment a;
        while (stolen.size() < MAX_NUMBER_STOLEN_ASSIGNMENTS && (a = availableAssignments.poll()) != null) {
            stolen.add(a);
        }

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

        long totalObjectStolen = 0l;
        for (final Assignment s : stolen) {
            final DistColAssignment assignment = (DistColAssignment) s;
            totalObjectStolen += assignment.range.size();

            final long upperBound = assignment.range.to;
            assignment.progress.entrySet().forEach((entry) -> {
                final Progress progress = entry.getValue();
                final GlbOperation op = entry.getKey();
                if (progress.next < upperBound) { // If the operation has work left
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

        // Log the transfer
        GlbComputer.getComputer().logger.put(LOGKEY_GLB, "DistCol#LifelineAnswer;" + totalObjectStolen,
                Long.toString(System.nanoTime()));

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

            if (assignmentsLeftToProcess.get(operation).addAndGet(-removedAssignments) == 0) { // Decrements the counter
                // All assignments for this operation have completed locally
                GlbComputer.getComputer().signalLocalOperationCompletion(operation);
            }
        }

        return true;
    }

    @Override
    public Assignment assignWorkToWorker() {
        return availableAssignments.poll();
    }

    /**
     * Merges the given assignments into this GlbTask. This method is called by a
     * lifeline answer after the instances on which the assignment operate have been
     * integrated into the underlying {@link DistChunkedList}.
     *
     * @param quantities  the number of assignment which have work for each glb
     *                    operation entered as a key in this map
     * @param assignments the assignments that were stolen
     */
    @Override
    @SuppressWarnings("rawtypes")
    public void mergeAssignments(HashMap<GlbOperation, Integer> quantities, ArrayList<Assignment> assignments) {
        long totalReceivedObjects = 0l;

        // As the first part and before placing assignments into the queues, we
        // increment the counters for the number of assignments left to process for each
        // operation. This can be done without any particular protections.
        for (final Map.Entry<GlbOperation, Integer> entry : quantities.entrySet()) {
            final AtomicInteger i = assignmentsLeftToProcess.get(entry.getKey());
            assertNotNull(i);
            i.addAndGet(entry.getValue());
        }

        // All all assignments to the "availableAssignments" collection
        for (final Assignment a : assignments) {
            final DistColAssignment dca = (DistColAssignment) a; // Cast to the proper type
            dca.setParent(this); // From now on, "this" DistColGlbTask is handling the assignment
            availableAssignments.add(dca);
            totalReceivedObjects += dca.range.size();
        }

        // We log the number of objects received
        GlbComputer.getComputer().logger.put(LOGKEY_GLB, "DistCol#LifelineReceived;" + totalReceivedObjects,
                Long.toString(System.nanoTime()));
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
     * @param ops the new operations to process.
     * @return true if some new work is available on the local host as a result of
     *         this new operation. A case where this method would return false is if
     *         there were no elements in the local handle of
     *         {@link DistChunkedList}.
     */
    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public boolean newOperations(GlbOperation... ops) {
        assertTrue(availableAssignments.isEmpty());
        assignmentsLeftToProcess.clear();

        // Create an atomic counter for each operation in the batch
        final int numberOfAssignments = collection.ranges().size();
        for (final GlbOperation op : ops) {
            assignmentsLeftToProcess.put(op, new AtomicInteger(numberOfAssignments));
        }

        // Create a new assignment for each range of the underlying collection
        final Collection<LongRange> ranges = collection.getAllRanges();
        ranges.forEach((lr) -> {
            final DistColAssignment a = new DistColAssignment(lr, this);
            // Add a tracker in the assignment for each operation in the batch
            for (final GlbOperation op : ops) {
                a.progress.put(op, new Progress(a.range.from));
            }
            a.updatePriority();
            availableAssignments.add(a);
        });

        return !ranges.isEmpty();
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
