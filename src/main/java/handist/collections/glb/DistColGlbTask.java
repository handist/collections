package handist.collections.glb;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.function.SerializableConsumer;

public class DistColGlbTask implements GlbTask {
	/**
	 * Class describing the progress of the various operations taking place on a
	 * range pertaining to the {@link handist.collections.dist.DistCol}
	 *
	 * @author Patrick
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
		 * @return true if there is some work remaining in the operation that was
		 *         progressed, false if the operation that was chosen was completed on
		 *         this fragment
		 */
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		public boolean process(int qtt) {

			// TODO this method of selecting the operation needs improvements
			// We may be able to introduce some sort of priority mechanism here
			GlbOperation op = null;
			long next = Long.MAX_VALUE;
			for (final Map.Entry<GlbOperation, Long> e : progress.entrySet()) {
				if ((next = e.getValue()) < range.to) {
					// We pick this operation
					op = e.getKey();
					break;
				}
			}

			long limit = next + qtt;
			boolean operationCompletedForThisAssignment = false;
			if (limit >= range.to) {
				// The operation selected will be completed on this assignment
				limit = range.to;
				operationCompletedForThisAssignment = true;
			}
			progress.put(op, limit);

			// Computation loop
			final SerializableConsumer action = op.operation;
			for (long l = next; l < limit; l++) {
				action.accept(l); 	// Unchecked. But I know that the action for a DistCol takes a long as parameter.
			}

			// Signal the parent GlbTask that the operation has completed on this
			// assignment.
			if (operationCompletedForThisAssignment) {
				operationTerminatedOnAssignment(op);
				// Depending if there are other operations on this assignment, we
				// place it the appropriate collection of the parent DistColGlbTask
				// FIXME this needs to be protected against calls to #newOperation
				if (operationRemaining()) {
					assignedAssignments.remove(this);
					availableAssignments.add(this);
				} else {
					assignedAssignments.remove(this);
					completedAssignments.add(this);
				}
			}

			return !operationCompletedForThisAssignment;
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
	 * Map which associates the number of completed assignments to each operation in
	 * progress.
	 * <p>
	 * As workers complete assignments, they will increment the atomic counter. When
	 * a worker completes the last assignment available for an operation, local
	 * completion of this operation is reached. That worker will therefore trigger
	 * the stealing process by releasing the operation thread which is currently
	 * blocking on a {@link SemaphoreBlocker}.
	 * <p>
	 * This member does not count how many {@link Assignment} are currently in
	 * member {@link #completedAssignments}. The integer in this collection and the
	 * number of assignments in {@link #completedAssignments} would match if there
	 * is a single operation operating on the underlying {@link DistCol} instance,
	 * but they might not if there are more operations underway.
	 */
	@SuppressWarnings("rawtypes")
	Map<GlbOperation, AtomicInteger> completedAssignmentsCounter;
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
		completedAssignmentsCounter = new HashMap<>();

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
	public synchronized Assignment assignWorkToWorker() {
		final DistColAssignment a = availableAssignments.poll();
		if (a != null) {
			assignedAssignments.add(a);
			return a;
		} else {
			return null;
		}
	}

	/**
	 * Initializes the iterator for every assignment contained in this local
	 * instance.
	 * <p>
	 * This method is synchronized to prevent any worker moving an assignment from
	 * {@link #availableAssignments} to {@link #assignedAssignments}, or from
	 * {@link #assignedAssignments} to #CompletedAssignments.
	 *
	 * @param op the new operation available for processing
	 * @return true if at least one of the {@link Assignment} held locally was
	 *         prepared for the new operation. This method will return false if
	 *         there were no {@link Assignment} locally at the time the method was
	 *         called.
	 */
	@Override
	public synchronized boolean newOperation(@SuppressWarnings("rawtypes") GlbOperation op) {
		// We allocate an extra counter for completed assignments
		// This counter is used in #operationTerminatedOnAssignment(GlbOperation)
		// to check if all assignments of the DistColGlbTask have been performed
		completedAssignmentsCounter.put(op, new AtomicInteger(0));

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
		final AtomicInteger ai = completedAssignmentsCounter.get(op);
		final int completedAssignments = ai.incrementAndGet();

		if (completedAssignments == totalAssignments.get()) {
			final GlbComputer glb = GlbComputer.getComputer();

			// This was the last assignment for this operation
			// We unblock the operation thread that was waiting
			glb.reserve.blockers.get(op).unblock(); // TODO possible to have a null pointer exception here

			// We also signal the local reserve instance that this operation has completed
			// locally.
			glb.reserve.tasksWithWork.remove(op);
		}
	}
}
