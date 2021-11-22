package handist.collections.accumulator;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.hazelcast.util.function.BiConsumer;

import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;

/**
 * Class representing a factory for {@link ChunkedList}&lt;U&gt; instances
 * dedicated to a single thread.
 * <p>
 * This allows for multiple threads to record information about modification
 * (recoded in the shape of a U object) to be made on each element of a
 * {@link ChunkedList}&lt;T&gt;.
 * <p>
 * There is currently one implementation of {@link ThreadLocalAccumulator}:
 * <ul>
 * <li>{@link AccumulatorCompleteRange} which prepares
 * {@link ChunkedList}&lt;U&gt; containing all ranges of the target
 * {@link ChunkedList}&lt;T&gt; for every threads. Use this variant if you known
 * that the elements assigned to each thread needs to record information about
 * all indices contained in the target {@link ChunkedList}&lt;T&gt;.
 * </ul>
 *
 * @author Kawanishi Yoshiki
 *
 * @param <T> type contained by the {@link ChunkedList} targeted by the
 *            computation
 *
 * @param <R> type used to keep information about the modification to be
 *            performed on the target {@link ChunkedList}
 */
/*
 * <li>{@link AccumulatorRequiredRange} which prepares {@link
 * ChunkedList}&lt;U&gt; with only the exact ranges explicitly asked for through
 * method {@link #acquire(LongRange)}. Use this variant if you know in advance
 * which range(s) of the target {@link ChunkedList}&lt;T&gt; each thread will
 * access. <li>{@link AccumulatorBlockRange} which prepares {@link
 * ChunkedList}&lt;U&gt; with initializes ranges within a certain neighborhood
 * around the indices asked for through method {@link #acquire(LongRange)}. This
 * helps in cases where each thread is unlikely to need to record information
 * about all indices in the target {@link ChunkedList}&lt;T&gt; but is likely to
 * need to record information for multiple objects in somewhat predictable
 * patterns. </ul>
 */
public abstract class ThreadLocalAccumulator<T, R> {

    /**
     * Collection which keeps track of the {@link ChunkedList} created for each
     * thread participating in the computation.
     */
    protected final ConcurrentHashMap<Thread, ChunkedList<R>> accumulators;

    /**
     * Underlying ChunkedList on which this {@link ThreadLocalAccumulator} operates
     */
    public final ChunkedList<T> target;

    /**
     * Reduction function used to apply modifications to the T type based on
     * information contained in the R type
     */
    public final BiConsumer<T, R> reduceFunc;

    /**
     * Function used to create the R type based on the index of the corresponding T
     * type
     */
    public final Function<Long, R> initFunc;

    /**
     * Protected constructor. Initializes members common to all
     * ThreadLocalAccumulator implementations
     *
     * @param targetChunkedList   chunked list on which this
     *                            {@link ThreadLocalAccumulator} will operate
     * @param initializerFunction function used to initialize a new R type for the T
     *                            object located at the specified index
     * @param reduction           function used to reduce the R type into T
     */
    protected ThreadLocalAccumulator(ChunkedList<T> targetChunkedList, Function<Long, R> initializerFunction,
            BiConsumer<T, R> reduction) {
        target = targetChunkedList;
        reduceFunc = reduction;
        initFunc = initializerFunction;
        accumulators = new ConcurrentHashMap<>();
    }

    /**
     * Returns the {@link ChunkedList}&lt;R&gt; prepared specifically for the caller
     * thread. All threads using this accumulator must call this method to obtain
     * their dedicated {@link ChunkedList}.
     * <p>
     * In principle, creates a new {@link ChunkedList}&lt;R&gt; when first called in
     * the current thread. Subsequent calls will return the same
     * {@link ChunkedList}. The detail of the {@link Chunk}s contained in the
     * returned {@link ChunkedList} depend on the implementation chosen.
     */
    public abstract ChunkedList<R> acquire(LongRange range);

    /**
     * For internal debugging purposes only. Do not call this method.
     */
    abstract void debugPrint();

    /**
     * Performs the specified action on the pairs of thread-local
     * {@link ChunkedList} R objects and the T objects of matching indices part of
     * the target {@link ChunkedList}.
     * <p>
     * FIXME this method is not equipped to deal with cases where there are gaps in
     * the ranges contained by the thread-local ChunkedList&lt;R&gt;. Such
     * situations currently result in unpredictable behaviors. TODO this method will
     * not fast because of call {@link ChunkedList#subList}.
     *
     * @param range  the range of indices onto which the action should be applied.
     * @param action the action to perform on each pair of T and R objects
     */
    public void forEachWithTarget(LongRange range, BiConsumer<T, R> action) {
        final Iterator<T> targetIter = target.subList(range).iterator();
        final Iterator<R> accIter = acquire(range).subList(range).iterator();
        while (accIter.hasNext()) {
            action.accept(targetIter.next(), accIter.next());
        }
    }

    /**
     * Applies the {@link #reduceFunc} on all pairs of thread-local R objects and
     * the T objects at matching index in the {@link #target} {@link ChunkedList}.
     */
    /*
     * FIXME this method will not work if there are gaps in the target chunkedList
     * that for some reason are present in a Thread-local ChunkedList.
     */
    public void merge() {
        accumulators.forEach((th, acc) -> {
            acc.forEachChunk((c) -> {
                final Iterator<R> iter = c.iterator();
                target.forEach(c.getRange(), (T t) -> {
                    reduceFunc.accept(t, iter.next());
                });
            });
        });
    }

    /**
     * Merge chunkedList&lt;R&gt; for every threads to target chunkedList&lt;T&gt;.
     *
     * @param nThreads : number of threads for reduction.
     */
    public abstract void parallelMerge(int nThreads);

    /**
     * Returns the ranges that the current thread has already initialized in its
     * dedicated {@link ChunkedList}&lt;R&gt;.
     *
     * @return collection of {@link LongRange} present in the dedicated
     *         {@link ChunkedList} for the current thread, {@code null} if this
     *         thread has no dedicated {@link ChunkedList} yet.
     */
    public Collection<LongRange> ranges() {
        final Thread thread = Thread.currentThread();
        final ChunkedList<R> threadLocalCL = accumulators.get(thread);
        if (threadLocalCL != null) {
            return threadLocalCL.ranges();
        } else {
            return null;
        }
    }

    /**
     * Discards all initialized {@link ChunkedList}s prepared up until this point.
     * Call this method if you plan to re-use this {@link ThreadLocalAccumulator}
     * again for another computation.
     */
    public void reset() {
        accumulators.clear();
    }
}
