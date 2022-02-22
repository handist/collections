package handist.collections.accumulator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Function;

import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;

/**
 * Variation of {@link Accumulator} which creates accumulators for each range
 * specified during the construction of this object.
 *
 * @param <A> the accumulator type used to store information
 */
public class AccumulatorCompleteRange<A> extends Accumulator<A> {

    private class CompleteRangeTLA implements ThreadLocalAccumulator<A> {

        ChunkedList<A> accumulators;

        public CompleteRangeTLA() {
            accumulators = new ChunkedList<>();
            for (final LongRange range : rangesToAllocate) {
                final Chunk<A> accChunk = new Chunk<>(range, initFunc);
                accumulators.add(accChunk);
            }
        }

        @Override
        public A acquire(long idx) {
            return accumulators.get(idx);
        }

        @Override
        public ChunkedList<A> acquire(LongRange range) {
            return accumulators.subList(range);
        }

        @Override
        public ChunkedList<A> getChunkedList() {
            return accumulators;
        }

        @Override
        public Collection<LongRange> ranges() {
            return accumulators.ranges();
        }
    }

    /**
     * Collection containing the ranges to allocate in each individual
     * ThreadLocalAccumulator managed by this {@link AccumulatorCompleteRange}
     * instance.
     */
    private final Collection<LongRange> rangesToAllocate;

    /**
     * Constructor. Allocates every ThreadLocalAccumulator with an individual
     * accumulator for every index contained in the ranges contained by the
     * {@link ChunkedList} given as parameter.
     * <p>
     * Note that subsequent addition of ranges to the {@link ChunkedList} <em>will
     * not</em> be reflected into the instance created by calling this constructor.
     * If the ranges contained by a {@link ChunkedList} change and this change needs
     * to be reflected by the {@link AccumulatorCompleteRange}, a new instance
     * should be created.
     *
     * @param toAllocate {@link ChunkedList} whose contained ranges will be
     *                   allocated for thread-local accumulators prepared by this
     *                   instance
     * @param initFunc   the function used to allocate the individual allocators
     */
    public AccumulatorCompleteRange(ChunkedList<?> toAllocate, Function<Long, A> initFunc) {
        this(toAllocate.ranges(), initFunc);
    }

    /**
     * Constructor. Allocates every ThreadLocalAccumulator with an individual
     * accumulator for every index contained in the ranges given as parameter.
     * <p>
     * It is assumed that the ranges given as parameter are mutually exclusive. In
     * other words, no two ranges should overlap. Otherwise, exceptions will be
     * thrown when using this accumulator.
     * <p>
     * Also, subsequent modifications to the collection given as parameter will not
     * influence the behavior of previously created {@link AccumulatorCompleteRange}
     * instance. If a {@link AccumulatorCompleteRange} is desired on a different set
     * of ranges, a new instance should be created.
     *
     * @param ranges the ranges on which this accumulator will be capable of
     *               accumulating values
     */
    public AccumulatorCompleteRange(Collection<LongRange> ranges, Function<Long, A> initFunc) {
        super(initFunc);
        rangesToAllocate = new ArrayList<>(ranges.size());
        rangesToAllocate.addAll(ranges);
    }

    @Override
    protected ThreadLocalAccumulator<A> newThreadLocalAccumulator() {
        return new CompleteRangeTLA();
    }
}
