package handist.collections.accumulator;

import static apgas.Constructs.*;

import java.util.List;
import java.util.function.Function;

import com.hazelcast.util.function.BiConsumer;

import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;
import handist.collections.RangedList;

/**
 * Variation of {@link ThreadLocalAccumulator} which creates dedicated
 * {@link ChunkedList} for each thread with the same chunk ranges as the target
 * {@link ChunkedList}.
 *
 * @param <T> The type of the target {@link ChunkedList}.
 * @param <R> The type used to store information in relation to each individual
 *            T elemement of the targeted {@link ChunkedList}.
 */
public class AccumulatorCompleteRange<T, R> extends ThreadLocalAccumulator<T, R> {

    AccumulatorCompleteRange(ChunkedList<T> target, Function<Long, R> initFunc, BiConsumer<T, R> reduceFunc) {
        super(target, initFunc, reduceFunc);
    }

    /**
     * Returns the {@link ChunkedList}&lt;R&gt; dedicated the calling thread.
     * <p>
     * Creates a new {@link ChunkedList}&lt;R&gt; when first called by the current
     * thread. The returned {@link ChunkedList} will contain the range specified as
     * parameter pre-initialized with the initializer function given to the
     * constructor as parameter
     */
    @Override
    public ChunkedList<R> acquire(LongRange range) {
        final Thread thread = Thread.currentThread();
        // Atomically create new ChunkedList if not created before for this thread
        final ChunkedList<R> toReturn = accumulators.computeIfAbsent(thread, k -> {
            return new ChunkedList<>();
        });

        // If the chunk was just created, also insert the necessary ranges
        if (toReturn.isEmpty() && !target.isEmpty()) {
            target.ranges().forEach((LongRange r) -> {
                toReturn.add(new Chunk<>(r, initFunc));
            });
        }

        // Return the created ChunkedList as is without calling subList.
        return toReturn;
    }

    @Override
    void debugPrint() {
        accumulators.forEach((thread, chunkedList) -> {
            System.out.println("[AccumulatorCompleteRange] Thread " + thread.getId() + " : " + chunkedList.ranges());
        });
    }

    @Override
    public void merge() {
        accumulators.forEach((th, acc) -> {
            acc.forEachChunk((c) -> {
                // Obtain the matching Chunk in the target
                final LongRange accumulatorRange = c.getRange();
                final RangedList<T> targetChunk = target.getChunk(accumulatorRange);

                if (targetChunk == null || !targetChunk.getRange().equals(accumulatorRange)) {
                    System.err.println("[ThreadLocalAccumulator#merge] the target ChunkedList does not contain range "
                            + accumulatorRange + ", skipping this range");
                } else {
                    targetChunk.forEach(accumulatorRange, c, (t1, c1) -> {
                        reduceFunc.accept(t1, c1);
                    });
                }
            });
        });
    }

    @Override
    public void parallelMerge(int nThreads) {
        final List<ChunkedList<T>> split = target.separate(nThreads);
        finish(() -> {
            for (final ChunkedList<T> split0 : split) {
                async(() -> {
                    split0.forEachChunk((chunk) -> {
                        accumulators.forEach((thread, copy) -> {
                            final RangedList<R> copy1 = copy.subList1(chunk.getRange());
                            chunk.forEach(chunk.getRange(), copy1, (t1, c1) -> {
                                reduceFunc.accept(t1, c1);
                            });
                        });
                    });
                });
            }
        });
    }
}
