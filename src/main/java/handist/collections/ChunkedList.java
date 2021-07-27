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
package handist.collections;

import static apgas.Constructs.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Spliterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import handist.collections.FutureN.ReturnGivenResult;
import handist.collections.dist.Reducer;
import handist.collections.function.LongTBiConsumer;

/**
 * Large collection containing multiple {@link Chunk}s. This overcomes the
 * storing limitation of individual {@link Chunk}s. A {@link ChunkedList} can
 * hold multiple {@link Chunk}s, adjacent or not. However, it cannot contain
 * Chunks whose bounds overlap. This is necessary to avoid having potentially
 * multiple values associated with a single ({@code long}) index.
 *
 * @param <T> The type of the elements handled by the {@link Chunk}s the
 *            {@link ChunkedList} contains, and by extension, the type of
 *            elements handled by the {@link ChunkedList}
 */
public class ChunkedList<T> implements Iterable<T>, Serializable {

    /**
     * Iterator class for {@link ChunkedList}. Iterates on two levels between the
     * chunks contained in the {@link ChunkedList} and the elements contained in the
     * {@link Chunk}s.
     *
     * @param <S> type of the elements handled by the {@link ChunkedList}
     */
    private static class It<S> implements Iterator<S> {
        public ConcurrentSkipListMap<LongRange, RangedList<S>> chunks;
        private Iterator<S> cIter;
        private LongRange range;

        public It(ConcurrentSkipListMap<LongRange, RangedList<S>> chunks) {
            this.chunks = chunks;
            final Map.Entry<LongRange, RangedList<S>> firstEntry = chunks.firstEntry();
            if (firstEntry != null) {
                final RangedList<S> firstChunk = firstEntry.getValue();
                range = firstChunk.getRange();
                cIter = firstChunk.iterator();
            } else {
                range = null;
                cIter = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (range == null) {
                return false;
            }
            if (cIter.hasNext()) {
                return true;
            }
            final Map.Entry<LongRange, RangedList<S>> nextEntry = chunks.higherEntry(range);
            if (nextEntry == null) {
                range = null;
                cIter = null;
                return false;
            }
            range = nextEntry.getKey();
            cIter = nextEntry.getValue().iterator();
            return cIter.hasNext();
        }

        @Override
        public S next() {
            if (hasNext()) {
                return cIter.next();
            }
            throw new IndexOutOfBoundsException();
        }

    }

    /**
     * Class which defines the order in which Chunks are stored in the underlying
     * {@link ConcurrentSkipListMap}. Contrary to the default ordering of class
     * {@link LongRange}, entries in this member will be sorted by increasing
     * {@link LongRange#from} and <em>decreasing</em> {@link LongRange#to}. This
     * simplifies a number of retrieval operations proposed by class
     * {@link ChunkedList} as retrieving a target range
     *
     * @author Patrick Finnerty
     *
     */
    public static final class LongRangeOrdering implements Comparator<LongRange>, Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = 8092975204762862773L;

        @Override
        public int compare(LongRange arg0, LongRange arg1) {
            final int fromComparison = (int) (arg0.from - arg1.from);
            return (int) ((fromComparison) == 0 ? arg1.to - arg0.to : fromComparison);
        }

    }

    public static class UnmodifiableView<S> extends ChunkedList<S> {
        /**
         *
         */
        private static final long serialVersionUID = 7195102432562086394L;
        ChunkedList<S> base;

        public UnmodifiableView(ChunkedList<S> base) {
            this.base = base;
        }

        @Override
        public void add(RangedList<S> c) {
            throw new UnsupportedOperationException("UmmodifiableView does not support add()");
        }

        @Override
        public void add_unchecked(RangedList<S> c) {
            throw new UnsupportedOperationException("UmmodifiableView does not support add_unchecked()");
        }

        @Override
        public <U> void asyncForEach(BiConsumer<? super S, Consumer<? super U>> action,
                ParallelReceiver<? super U> toStore) {
            base.asyncForEach(action, toStore);
        }

        @Override
        public void asyncForEach(Consumer<? super S> action) {
            base.asyncForEach(action);
        }

        @Override
        public <U> Future<ChunkedList<S>> asyncForEach(ExecutorService pool, int nthreads,
                BiConsumer<? super S, Consumer<? super U>> action, ParallelReceiver<? super U> toStore) {
            return base.asyncForEach(pool, nthreads, action, toStore);
        }

        @Override
        @Deprecated
        public Future<ChunkedList<S>> asyncForEach(ExecutorService pool, int nthreads, Consumer<? super S> action) {
            return base.asyncForEach(pool, nthreads, action);
        }

        @Override
        @Deprecated
        public Future<ChunkedList<S>> asyncForEach(ExecutorService pool, int nthreads,
                LongTBiConsumer<? super S> action) {
            return base.asyncForEach(pool, nthreads, action);
        }

        @Override
        public void asyncForEach(LongTBiConsumer<? super S> action) {
            base.asyncForEach(action);
        }

        @Override
        public <S1> Future<ChunkedList<S1>> asyncMap(ExecutorService pool, int nthreads,
                Function<? super S, ? extends S1> func) {
            return base.asyncMap(pool, nthreads, func);
        }

        @Override
        public boolean attemptSplitChunkAtSinglePoint(LongRange lr) {
            throw new UnsupportedOperationException("UmmodifiableView does not support split operations.");
        }

        @Override
        public boolean attemptSplitChunkAtTwoPoints(LongRange lr) {
            throw new UnsupportedOperationException("UmmodifiableView does not support split operations.");
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException("UmmodifiableView does not support clear().");
        }

        @Override
        public Object clone() {
            return base.clone();
        }

        @Override
        public boolean contains(Object o) {
            return base.contains(o);
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return base.containsAll(c);
        }

        @Override
        public boolean containsChunk(RangedList<S> c) {
            return base.containsChunk(c);
        }

        @Override
        public boolean containsIndex(long i) {
            return base.containsIndex(i);
        }

        @Override
        public boolean containsRange(LongRange range) {
            return base.containsRange(range);
        }

        @Override
        public boolean equals(Object o) {
            return base.equals(o);
        }

        @Override
        public List<RangedList<S>> filterChunk(Predicate<RangedList<? super S>> filter) {
            return base.filterChunk(filter);
        }

        @Override
        public <U> void forEach(BiConsumer<? super S, Consumer<? super U>> action, Collection<? super U> toStore) {
            base.forEach(action, toStore);
        }

        @Override
        public <U> void forEach(BiConsumer<? super S, Consumer<? super U>> action, Consumer<? super U> receiver) {
            base.forEach(action, receiver);
        }

        @Override
        public <U> void forEach(BiConsumer<? super S, Consumer<? super U>> action,
                ParallelReceiver<? super U> toStore) {
            base.forEach(action, toStore);
        }

        @Override
        public void forEach(Consumer<? super S> action) {
            base.forEach(action);
        }

        @Override
        @Deprecated
        public <U> void forEach(ExecutorService pool, int nthreads, BiConsumer<? super S, Consumer<? super U>> action,
                ParallelReceiver<U> toStore) {
            base.forEach(pool, nthreads, action, toStore);
        }

        @Override
        @Deprecated
        public void forEach(ExecutorService pool, int nthreads, Consumer<? super S> action) {
            base.forEach(pool, nthreads, action);
        }

        @Override
        @Deprecated
        public void forEach(ExecutorService pool, int nthreads, LongTBiConsumer<? super S> action) {
            base.forEach(pool, nthreads, action);
        }

        @Override
        public void forEach(LongRange range, Consumer<? super S> action) {
            base.forEach(range, action);
        }

        @Override
        public void forEach(LongRange range, LongTBiConsumer<? super S> action) {
            base.forEach(range, action);
        }

        @Override
        public void forEach(LongTBiConsumer<? super S> action) {
            base.forEach(action);
        }

        @Override
        public void forEachChunk(Consumer<RangedList<S>> op) {
            base.forEachChunk(op);
        }

        @Override
        public void forEachChunk(LongRange range, Consumer<RangedList<S>> op) {
            base.forEachChunk(range, op);
        }

        @Override
        public S get(long i) {
            return base.get(i);
        }

        @Override
        public RangedList<S> getChunk(LongRange lr) {
            return base.getChunk(lr);
        }

        @Override
        public int hashCode() {
            return base.hashCode();
        }

        @Override
        public boolean isEmpty() {
            return base.isEmpty();
        }

        @Override
        public Iterator<S> iterator() {
            return base.iterator();
        }

        @Override
        public <S1> ChunkedList<S1> map(ExecutorService pool, int nthreads, Function<? super S, ? extends S1> func) {
            return base.map(pool, nthreads, func);
        }

        @Override
        public <S1> ChunkedList<S1> map(Function<? super S, ? extends S1> func) {
            return base.map(func);
        }

        @Override
        public int numChunks() {
            return base.numChunks();
        }

        @Override
        public <U> void parallelForEach(BiConsumer<? super S, Consumer<? super U>> action,
                ParallelReceiver<? super U> toStore) {
            base.parallelForEach(action, toStore);
        }

        @Override
        public void parallelForEach(Consumer<? super S> action) {
            base.parallelForEach(action);
        }

        @Override
        public void parallelForEach(LongTBiConsumer<? super S> action) {
            base.parallelForEach(action);
        }

        @Override
        public Collection<LongRange> ranges() {
            return base.ranges();
        }

        @Override
        public <R extends Reducer<R, S>> R reduce(R reducer) {
            return base.reduce(reducer);
        }

        @Override
        public <R extends Reducer<R, RangedList<S>>> R reduceChunk(R reducer) {
            return base.reduceChunk(reducer);
        }

        @Override
        public RangedList<S> remove(LongRange range) {
            throw new UnsupportedOperationException("UmmodifiableView does not support remove operations.");
        }

        @Override
        @Deprecated
        public RangedList<S> remove(RangedList<S> c) {
            throw new UnsupportedOperationException("UmmodifiableView does not support remove operations.");
        }

        @Override
        public List<ChunkedList<S>> separate(int n) {
            throw new UnsupportedOperationException("UmmodifiableView does not support separate operations.");
        }

        @Override
        public S set(long i, S value) {
            return base.set(i, value);
        }

        @Override
        public long size() {
            return base.size();
        }

        @Override
        public ArrayList<RangedList<S>> splitChunks(LongRange range) {
            throw new UnsupportedOperationException("UmmodifiableView does not support split operations.");
        }

        @Override
        public Spliterator<S> spliterator() {
            return base.spliterator();
        }

        @Override
        public ChunkedList<S> subList(LongRange range) {
            return new UnmodifiableView<>(base.subList(range));
        }

        @Override
        public String toString() {
            return base.toString();
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 6899796587031337979L;

    /**
     * Chunks contained by this instance. They are sorted using the
     * {@link LongRange} ordering.
     */
    protected final ConcurrentSkipListMap<LongRange, RangedList<T>> chunks;

    /**
     * Running tally of how many elements can be contained in the ChunkedList. It is
     * equal to the sum of the size of each individual chunk.
     */
    private final AtomicLong size;

    /**
     * Default constructor. Prepares the contained for the {@link Chunk}s this
     * instance is going to receive.
     */
    public ChunkedList() {
        chunks = new ConcurrentSkipListMap<>(new LongRangeOrdering());
        size = new AtomicLong(0l);
    }

    /**
     * Constructor which takes an initial {@link ConcurrentSkipListMap} of
     * {@link LongRange} mapped to {@link RangedList}.
     *
     * @param chunks initial mappings of {@link LongRange} and {@link Chunk}s
     * @throws IllegalArgumentException if the provided chunks do not follow the
     *                                  custom ordering used by {@link ChunkedList}.
     */
    public ChunkedList(ConcurrentSkipListMap<LongRange, RangedList<T>> chunks) {
        this.chunks = chunks;
        if (!(chunks.comparator() instanceof LongRangeOrdering)) {
            throw new IllegalArgumentException("The provided chunks does not follow the correct ordering");
        }
        long accumulator = 0l;
        for (final LongRange r : chunks.keySet()) {
            accumulator += r.size();
        }
        size = new AtomicLong(accumulator);
    }

    /**
     * Add a chunk to this instance. The provided chunk should not intersect with
     * any other already present in this instance, a {@link RuntimeException} will
     * be thrown otherwise.
     *
     * @param c the chunk to add to this instance
     * @throws RuntimeException if the range on which the provided {@link Chunk} is
     *                          defined intersects with another {@link Chunk}
     *                          already present in this instance
     */
    public void add(RangedList<T> c) {
        final LongRange desired = c.getRange();
        final LongRange intersection = checkOverlap(desired);
        if (intersection != null) {
            throw new ElementOverlapException("LongRange " + desired + " overlaps " + intersection
                    + " which is already present in this ChunkedList");
        }
        chunks.put(desired, c);
        size.addAndGet(c.size());
    }

    /**
     * Places the given ranged list without performing any checks.
     * <p>
     * This is useful, particularly when splitting chunks as cannot afford a moment
     * between which the original chunk is removed from the collection and the newer
     * chunks are placed into the collection. As the newer chunks need to be placed
     * first, for a brief instant, the ChunkedList will contain multiple ranges
     * which overlap.
     *
     * @param c the chunk to place in the collection
     */
    protected void add_unchecked(RangedList<T> c) {
        chunks.put(c.getRange(), c);
        size.addAndGet(c.size());
    }

    public <U> void asyncForEach(BiConsumer<? super T, Consumer<? super U>> action,
            final ParallelReceiver<? super U> toStore) {
        asyncForEach(defaultParallelism(), action, toStore);
    }

    public void asyncForEach(Consumer<? super T> action) {
        asyncForEach(defaultParallelism(), action);
    }

    /**
     * Performs the provided action on every element in the collection
     * asynchronously using the provided executor service and the specified degree
     * of parallelism. This method returns a {@link FutureN.ReturnGivenResult} which
     * will wait on every asynchronous task spawned to complete before returning
     * this instance. The provided action may initialize or extract a type U from
     * the data contained in individual elements and give this type U to its second
     * parameter (a {@link Consumer} of U) which will in turn place these instances
     * in {@code toStore}. Note that if you do not need to extract any information
     * from the elements in this collection, you should use method
     * {@link #asyncForEach(ExecutorService, int, Consumer)} instead.
     *
     * @param <U>      the type of the information to extract from the instances
     * @param pool     executor service in charge or performing the operation
     * @param nthreads the degree of parallelism for this action, corresponds to the
     *                 number of pieces in which this instance contents will be
     *                 split to be handled by parallel threads
     * @param action   to action to perform on each individual element contained in
     *                 this instance, which may include placing a newly created
     *                 instance of type U into the {@link Consumer} (second
     *                 parameter of the lambda expression).
     * @param toStore  instance which supplies the {@link Consumer} used the lambda
     *                 expression to every parallel thread and will collect all the
     *                 U instances given to those {@link Consumer}s.
     * @return a {@link ReturnGivenResult} which waits on the completion of all
     *         asynchronous tasks before returning this instance. Programmers should
     *         also wait on the completion of this {@link FutureN} to make sure that
     *         no more U instances are placed into {@code toStore}.
     */
    @Deprecated
    public <U> Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads,
            BiConsumer<? super T, Consumer<? super U>> action, final ParallelReceiver<? super U> toStore) {
        final List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
            sub.forEach(action, toStore.getReceiver());
        });
        return new FutureN.ReturnGivenResult<>(futures, this);
    }

    /**
     * Performs the provided action on every element in the collection
     * asynchronously using the provided executor service and the specified degree
     * of parallelism. This method returns a {@link FutureN.ReturnGivenResult} which
     * will wait on every asynchronous task spawned to complete before returning
     * this instance.
     *
     * @param pool     executor service in charge or performing the operation
     * @param nthreads the degree of parallelism for this action, corresponds to the
     *                 number of pieces in which this instance contents will be
     *                 split to be handled by parallel threads
     * @param action   to action to perform on each individual element contained in
     *                 this instance
     * @return a {@link ReturnGivenResult} which waits on the completion of all
     *         asynchronous tasks before returning this instance
     */
    @Deprecated
    public Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads, Consumer<? super T> action) {
        final List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
        return new FutureN.ReturnGivenResult<>(futures, this);
    }

    /**
     * Performs the provided action on every (long) key and (T) value in the
     * collection asynchronously using the provided executor service and the
     * specified degree of parallelism. This method returns a
     * {@link FutureN.ReturnGivenResult} which will wait on every asynchronous task
     * spawned to complete before returning this instance.
     *
     * @param pool     executor service in charge or performing the operation
     * @param nthreads the degree of parallelism for this action, corresponds to the
     *                 number of pieces in which this instance contents will be
     *                 split to be handled by parallel threads
     * @param action   to action to perform on each pair of ({@code long} key and
     *                 (T) element contained in this instance
     * @return a {@link ReturnGivenResult} which waits on the completion of all
     *         asynchronous tasks before returning this instance
     */
    @Deprecated
    public Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads, LongTBiConsumer<? super T> action) {
        final List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
        return new FutureN.ReturnGivenResult<>(futures, this);
    }

    public <U> void asyncForEach(int parallelism, BiConsumer<? super T, Consumer<? super U>> action,
            final ParallelReceiver<? super U> toStore) {
        forEachParallelBody(parallelism, (ChunkedList<T> sub) -> {
            sub.forEach(action, toStore.getReceiver());
        });
    }

    public void asyncForEach(int parallelism, Consumer<? super T> action) {
        forEachParallelBody(parallelism, (ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
    }

    public void asyncForEach(int parallelism, LongTBiConsumer<? super T> action) {
        forEachParallelBody(parallelism, (ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
    }

    public void asyncForEach(LongTBiConsumer<? super T> action) {
        asyncForEach(defaultParallelism(), action);
    }

    /**
     * Creates a new ChunkedList defined on the same {@link LongRange}s as this
     * instance by performing the mapping function on each element contained in this
     * instance. This method applies the user-provided function in parallel using
     * the threads in the provided {@link ExecutorService} with the set degree of
     * parallelism by splitting the values contained in this instance into
     * equal-size portions. These portions correspond to futures that complete when
     * the portion has been dealt with. This method returns a
     * {@link ReturnGivenResult} which will return the newly created
     * {@link ChunkedList} once all the individual futures have completed.
     *
     * @param <S>      the type handled by the newly created map
     * @param pool     the executor service in charge of processing this ChunkedList
     *                 in parallel
     * @param nthreads the degree of parallelism desired for this operation
     * @param func     the mapping function from type T to type S from which the
     *                 elements of the {@link ChunkedList} to create will be
     *                 initialized
     * @return a {@link ReturnGivenResult} which will return the new
     *         {@link ChunkedList} once all parallel mapping operations have
     *         completed
     */
    public <S> Future<ChunkedList<S>> asyncMap(ExecutorService pool, int nthreads,
            Function<? super T, ? extends S> func) {
        final ChunkedList<S> result = new ChunkedList<>();
        final List<Future<?>> futures = mapParallelBody(pool, nthreads, func, result);

        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ParallelExecutionException("[ChunkedList] exception raised by worker threads.", e);
            }
        }
        return new FutureN.ReturnGivenResult<>(futures, result);
    }

    /**
     * Method used in preparation before transferring chunks. This method checks if
     * a chunk contained in this object has its range exactly matching the range
     * specified as parameter. If that is the case, returns {@code true}.
     * <p>
     * If that is not the case, i.e. a chunk held by this collection needs to be
     * split so that the specified range can be sent to a remote host, attempts to
     * make the split. If it is successful in splitting the existing chunk so that
     * the specified range has a corresponding chunk stored in this collection,
     * returns {@code true}. If splitting the existing range failed (due to a
     * concurrent attempts to split that range), returns {@code false}. The caller
     * of this method will have to call it again to attempt to make the check again.
     * <p>
     * The synchronizations in this method are made such that multiple calls to this
     * method will run concurrently, as long as different chunks are targeted for
     * splitting.
     * <p>
     * If two (or more) concurrent calls to this method target the same chunk, they
     * should be made with ranges that do not intersect. For instance, assuming this
     * collection holds a chunk mapped to range [0, 100). Calls to this method with
     * ranges [0,50) and [50, 75) and [90, 100) in whichever order (or concurrently)
     * is acceptable. However, calling this method with parameters [0, 50) and [25,
     * 75) is problematic as the second one to be made (or scheduled) will fail to
     * make the splits as the split points will be in two different chunks. However,
     * calling this method with parameters [0, 50) and later on with [25, 50) is
     * acceptable.
     *
     * @param lr the point at which there needs to be a change of chunk. This range
     *           needs to be empty, i.e. its members "from" and "to" need to be
     *           equal
     * @return {@code true} if the specified range can be safely sent to a remote
     *         place, {@code false} if this method needs to be called again to make
     *         it happen
     */
    protected boolean attemptSplitChunkAtSinglePoint(LongRange lr) {
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(lr);

        // It is possible for the requested point not to be present in any chunk
        if (entry == null || !entry.getKey().contains(lr.from)) {
            return true;
        }

        final LongRange chunkRange = entry.getKey();
        final boolean splitNeeded = chunkRange.from < lr.from && lr.from < chunkRange.to;

        if (!splitNeeded) {
            return true;
        }

        // Arrived here, we know that the chunk we have needs to be split
        // We synchronize on this specific Chunk
        synchronized (chunkRange) {
            // We restart the chunk acquisition process to check if we obtain the same chunk
            // If that is not the case, another thread has modified the chunks in the
            // ChunkedList and
            // this method has failed to do the modification, which will have to be
            // attempted again
            final Map.Entry<LongRange, RangedList<T>> checkEntry = chunks.floorEntry(lr);
            if (!entry.getKey().equals(checkEntry.getKey())) {
                return false;
            }

            // Check passed, we are the only thread which can split the targeted chunk
            final LinkedList<RangedList<T>> splittedChunks = entry.getValue().splitRange(lr.from);
            while (!splittedChunks.isEmpty()) {
                // It is important to insert the splitted chunks in reverse order.
                // Otherwise, parts of the original chunk would be shadowed due to the ordering
                // of Chunks used in ChunkedList, concurrently calling ChunkedList(or
                // DistCol)#get(long) would fail.
                add_unchecked(splittedChunks.pollLast());
            }
            remove(chunkRange);
            return true;
        }
    }

    /**
     * Method used in preparation before transferring chunks. This method checks if
     * a chunk contained in this object has its range exactly matching the range
     * specified as parameter. If that is the case, returns {@code true}.
     * <p>
     * If that is not the case, i.e. a chunk held by this collection needs to be
     * split so that the specified range can be sent to a remote host, attempts to
     * make the split. If it is successful in splitting the existing chunk so that
     * the specified range has a corresponding chunk stored in this collection,
     * returns {@code true}. If splitting the existing range failed (due to a
     * concurrent attempts to split that range), returns {@code false}. The caller
     * of this method will have to call it again to attempt to make the check again.
     * <p>
     * The synchronizations in this method are made such that multiple calls to this
     * method will run concurrently, as long as different chunks are targeted for
     * splitting.
     * <p>
     * If two (or more) concurrent calls to this method target the same chunk, they
     * should be made with ranges that do not intersect. For instance, assuming this
     * collection holds a chunk mapped to range [0, 100). Calls to this method with
     * ranges [0,50) and [50, 75) and [90, 100) in whichever order (or concurrently)
     * is acceptable. However, calling this method with parameters [0, 50) and [25,
     * 75) is problematic as the second one to be made (or scheduled) will fail to
     * make the splits as the split points will be in two different chunks. However,
     * calling this method with parameters [0, 50) and later on with [25, 50) is
     * acceptable.
     *
     * @param lr the range of entries which is going to be sent away. It is assumed
     *           that there exists a chunk in this collection which includes this
     *           provided range.
     * @return {@code true} if the specified range can be safely sent to a remote
     *         place, {@code false} if this method needs to be called again to make
     *         it happen
     */
    protected boolean attemptSplitChunkAtTwoPoints(LongRange lr) {
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(lr);

        final LongRange chunkRange = entry.getKey();
        final boolean leftSplit = chunkRange.from < lr.from;
        final boolean rightSplit = lr.to < chunkRange.to;

        long[] splitPoints;
        if (leftSplit && rightSplit) {
            splitPoints = new long[2];
            splitPoints[0] = lr.from;
            splitPoints[1] = lr.to;
        } else if (leftSplit) {
            splitPoints = new long[1];
            splitPoints[0] = lr.from;
        } else if (rightSplit) {
            splitPoints = new long[1];
            splitPoints[0] = lr.to;
        } else {
            return true;
        }

        // Arrived here, we know that the chunk we have needs to be split
        // We synchronize on this specific Chunk
        synchronized (chunkRange) {
            // We restart the chunk acquisition process to check if we obtain the same chunk
            // If that is not the case, another thread has modified the chunks in the
            // ChunkedList and
            // this method has failed to do the modification, which will have to be
            // attempted again
            final Map.Entry<LongRange, RangedList<T>> checkEntry = chunks.floorEntry(lr);
            if (!entry.getKey().equals(checkEntry.getKey())) {
                return false;
            }

            // Check passed, we are the only thread which can split the targeted chunk
            final LinkedList<RangedList<T>> splittedChunks = entry.getValue().splitRange(splitPoints);
            while (!splittedChunks.isEmpty()) {
                // It is important to insert the splitted chunks in reverse order.
                // Otherwise, parts of the original chunk would be shadowed due to the ordering
                // of Chunks used in ChunkedList, concurrently calling ChunkedList(or
                // DistCol)#get(long) would fail.
                add_unchecked(splittedChunks.pollLast());
            }
            remove(chunkRange);
            return true;
        }
    }

    /**
     * Checks if the provided {@link LongRange} intersects with the range of one of
     * the chunks contained by this instance. Returns the intersecting
     * {@link LongRange}, or {@code null} if there are no intersecting
     * {@link Chunk}s.
     *
     * @param range the LongRange instance to check
     * @return a LongRange on which one of the Chunks of this object is defined that
     *         intersects with the provided {@link LongRange}, or {@code null} if
     *         there are no such intersecting {@link Chunk}s
     */
    private LongRange checkOverlap(LongRange range) {
        return range.findOverlap(chunks);
    }

    /**
     * Removes all the chunks contained in this instance. This instance is
     * effectively empty as a result and a subsequent call to {@link #isEmpty()}
     * will return {@code true}, calling {@link #size()} will return {@code 0l}.
     */
    public void clear() {
        size.set(0l);
        chunks.clear();
    }

    /**
     * Returns a new {@link ChunkedList} which contains the same {@link Chunk}s as
     * this instance.
     *
     * @return a ChunkedList which holds the same Chunks as this instance
     */
    @Override
    protected Object clone() {
        final ConcurrentSkipListMap<LongRange, RangedList<T>> newChunks = new ConcurrentSkipListMap<>(
                new LongRangeOrdering());
        for (final RangedList<T> c : chunks.values()) {
            newChunks.put(c.getRange(), ((Chunk<T>) c).clone());
        }
        return new ChunkedList<>(newChunks);
    }

    /**
     * Checks if the provided object is contained within one of the Chunks this
     * instance holds. More formally, returns true if at least one of the chunks in
     * this instance contains at least one element 'e' such that (o==null ? e==null
     * : o.equals(e)). Of course, there may be several such elements 'e' in this
     * instance located in a single and/or multiple chunks.
     *
     * @param o object whose presence is to be checked
     * @return true if the provided object is contained in at least one of the
     *         chunks contained in this instance
     */
    public boolean contains(Object o) {
        for (final RangedList<T> chunk : chunks.values()) {
            if (chunk.contains(o)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Checks if all the elements provided in the collection are present in this
     * instance.
     * <p>
     * In its current implementation, it is equivalent to calling method
     * {@link #contains(Object)} with each element in the collection until either an
     * element is not found in this collection (at which point the method returns
     * {@code false} without checking the remaining objects in the collection) or
     * all the elements in the provided collection are found in this instance (at
     * which point this method returns {@code true}. If programmer can place the
     * elements that are more likely to be absent from this instance at the
     * beginning of the collection (in the order used by the {@link Iterator}, it
     * may save considerable execution time.
     *
     * @param c elements whose presence in this instance is to be checked
     * @return true if every instance in the provided collection is present in this
     *         collection
     */
    public boolean containsAll(Collection<?> c) {
        // cf
        // https://stackoverflow.com/questions/10199772/what-is-the-cost-of-containsall-in-java
        final Iterator<?> e = c.iterator();
        while (e.hasNext()) {
            if (!this.contains(e.next())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns whether this {@link ChunkedList} contains the given
     * {@link RangedList}.
     *
     * @param c the {@link RangedList} whose inclusion in this instance needs to be
     *          checked
     * @return {@code true} if the provided {@link RangedList} is contained in this
     *         instance, {@code false} otherwise
     */
    public boolean containsChunk(RangedList<T> c) {
        if (c == null) {
            return false;
        }
        // TODO I think we can do better than iterating over every entry in Chunks.
        // Iterating on the Chunks that intersect the range on which the 'c' given
        // as parameter is define would be an improvement.
        return chunks.containsValue(c);
    }

    public boolean containsIndex(long i) {
        final LongRange r = new LongRange(i);
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
        if (entry == null || !entry.getKey().contains(i)) {
//            entry = chunks.ceilingEntry(r);
//            if (entry == null || !entry.getKey().contains(i)) {
            return false;
//            }
        }
        return true;
    }

    public boolean containsRange(LongRange range) {
        // TODO same as method #containsChunk, this method needs improvements
        return range.contained(chunks);
    }

    private int defaultParallelism() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || !(o instanceof ChunkedList)) {
            return false;
        }
        // FIXME very slow
        final ChunkedList<?> target = (ChunkedList<?>) o;
        if (size() != target.size()) {
            return false;
        }
        for (final LongRange range : chunks.keySet()) {
            for (final long index : range) {
                final T mine = get(index);
                final Object yours = target.get(index);
                if (mine == null && yours != null) {
                    return false;
                }
                if (mine != null && !mine.equals(yours)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Returns the list of {@link Chunk} that pass the provided filter. Chunks are
     * included if the filter returns {@code true}.
     *
     * @param filter the filter deciding if a given chunk should be included in the
     *               returned list
     * @return the {@link Chunk}s contained in this instance which passed the
     *         provided filter
     */
    public List<RangedList<T>> filterChunk(Predicate<RangedList<? super T>> filter) {
        final List<RangedList<T>> result = new ArrayList<>();
        for (final RangedList<T> c : chunks.values()) {
            if (filter.test(c)) {
                result.add(c);
            }
        }
        return result;
    }

    /**
     * Performs the provided action on each element of this collection. As part of
     * this operation, some information of type U can be created or extracted from
     * elements and potentially stored into the provided collection using the
     * Consumer of U (second argument of the lambda expression). These elements will
     * be added using method {@link Collection#add(Object)}.
     * <p>
     * As a variant, you may also directly supply a Consumer&lt;U&gt; rather than a
     * collection using method {@link #forEach(BiConsumer, Consumer)}
     *
     * @param <U>     the type of the information to extract
     * @param action  action to perform on each element of the collection
     * @param toStore the collection in which the information extracted will be
     *                stored
     * @see #forEach(BiConsumer, Consumer)
     */
    public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, final Collection<? super U> toStore) {
        forEach(action, new Consumer<U>() {
            @Override
            public void accept(U u) {
                toStore.add(u);
            }
        });
    }

    /**
     * Performs the provided action o neach element of this collection. As part of
     * this operation, some information of type U can be created or extracted from
     * elements and given to the Consumer&lt;U&gt; (second argument of the lambda
     * expression). This {@link Consumer} available in the lambda expression is the
     * one given as second parameter of this method.
     * <p>
     * As an alternative, you can use method
     * {@link #forEach(BiConsumer, Collection)} to provide a {@link Collection}
     * rather than a {@link Consumer} as the second argument of the method.
     *
     * @param <U>      the type of the result extracted from the elements in this
     *                 collection
     * @param action   the action to perform on each element of this collection
     * @param receiver the receiver which will accept the U instances extracted from
     *                 the elements of this collection
     */
    public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, Consumer<? super U> receiver) {
        for (final RangedList<T> c : chunks.values()) {
            c.forEach(t -> action.accept(t, receiver));
        }
    }

    /**
     * Performs the provided action sequentially on the instances contained by this
     * {@link ChunkedList}, allowing for the by-product of the operation to be
     * stored in the specified {@link ParallelReceiver}.
     * <p>
     * This method is necessary as the manner in which instances are placed inside a
     * {@link ParallelReceiver} differs from that of a normal collection. Although
     * the features that handle parallel insertion of values are not leveraged in
     * this sequential method, the preparations needed to insert instances into the
     * {@link ParallelReceiver} remain necessary.
     *
     * @param <U>     the type of the data produced from the instances contained in
     *                this collection and stored in the provided
     *                {@link ParallelReceiver}
     * @param action  the action performed on all the elements contained in this
     *                collection. U instances may be created and given to the
     *                Consumer&lt;U&gt; as part of this action
     * @param toStore the parallel receiver which will receive all the U instances
     *                which are created as part of the action applied on the
     *                elements of this collection
     */
    public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, ParallelReceiver<? super U> toStore) {
        final Consumer<? super U> receiver = toStore.getReceiver();
        forEach(action, receiver);
    }

    /**
     * Performs the provided action on every element contained in this collection.
     *
     * @param action action to perform on each element contained in this instance
     */
    @Override
    public void forEach(Consumer<? super T> action) {
        for (final RangedList<T> c : chunks.values()) {
            c.forEach(action);
        }
    }

    /**
     * Performs the provided action on each element of this collection in parallel
     * using the provided {@link ExecutorService} with the specified degree of
     * parallelism. This action may involve extracting some information of type U
     * from individual elements and placing these into the Consumer (second argument
     * of the lambda expression). This {@link Consumer} used in the lambda exression
     * is obtained from the provided {@link ParallelReceiver} which will receive all
     * the U instances produced during this method. This method returns when all the
     * elements in the collection have been treated.
     *
     * @param <U>      type of the information extracted from individual elements
     * @param pool     executor service in charge of processing the elements of this
     *                 instance in parallel
     * @param nthreads degree of parallelism desired for this operation
     * @param action   action to perform on individual elements of this collection,
     *                 potentially extracting some information of type U and giving
     *                 it to the {@link Consumer}, the second argument of the action
     * @param toStore  {@link ParallelReceiver} instance which provides the
     *                 {@link Consumer}s of each thread that will process the
     *                 elements of this library and receive all the U elements
     *                 extracted from this collection
     */
    @Deprecated
    public <U> void forEach(ExecutorService pool, int nthreads, BiConsumer<? super T, Consumer<? super U>> action,
            final ParallelReceiver<U> toStore) {
        final List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
            sub.forEach(action, toStore.getReceiver());
        });
        waitNfutures(futures);
    }

    /**
     * Performs the provided action on every eleement in the collection in parallel
     * using the provided {@link ExecutorService} and the set degree of parallelism.
     * Returns when all operations have finished.
     *
     * @param pool     executor service in charge or performing the operation
     * @param nthreads the degree of parallelism for this action, corresponds to the
     *                 number of pieces in which this instance contents will be
     *                 split to be handled by parallel threads
     * @param action   to action to perform on element contained in this instance
     */
    @Deprecated
    public void forEach(ExecutorService pool, int nthreads, Consumer<? super T> action) {
        final List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
        waitNfutures(futures);
    }

    /**
     * Performs the provided action on every (long) key and (T) value in the
     * collection in parallel using the provided {@link ExecutorService} and the set
     * degree of parallelism. Returns when all operations have finished.
     *
     * @param pool     executor service in charge or performing the operation
     * @param nthreads the degree of parallelism for this action, corresponds to the
     *                 number of pieces in which this instance contents will be
     *                 split to be handled by parallel threads
     * @param action   to action to perform on each pair of ({@code long} key and
     *                 (T) element contained in this instance
     */
    @Deprecated
    public void forEach(ExecutorService pool, int nthreads, LongTBiConsumer<? super T> action) {
        final List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
        waitNfutures(futures);
    }

    /**
     * TODO : Still not sure if it works.
     */
    public void forEach(LongRange range, final Consumer<? super T> action) {
        subList(range).forEach(action);
    }

    /**
     * TODO : Still not sure if it works.
     */
    public void forEach(LongRange range, final LongTBiConsumer<? super T> action) {
        subList(range).forEach(action);
    }

    /**
     * Performs the provided action on every (long) key and (T) value in the
     * collection serquentially and returns.
     *
     * @param action to action to perform on each pair of ({@code long} key and (T)
     *               element contained in this instance
     */
    public void forEach(LongTBiConsumer<? super T> action) {
        for (final RangedList<T> c : chunks.values()) {
            c.forEach(c.getRange(), action);
        }
    }

    /**
     * Performs the provided operation on each {@link Chunk} contained in this
     * instance and returns.
     *
     * @param op operation to make on each chunk
     */
    public void forEachChunk(Consumer<RangedList<T>> op) {
        for (final RangedList<T> c : chunks.values()) {
            op.accept(c);
        }
    }

    /**
     * Performs the provided operation on each {@link Chunk} contained in this
     * instance and overlapped with the given range, then returns. Note that the
     * {@code op} receives {@link Chunk} that may not be contained in the range. If
     * the {@code range} is {@code null}, all the chunk will be scanned.
     *
     * This method dynamically scans the contained chunks. When searching the next
     * chunk, its search from the end of the previous chunk.
     *
     * @param range range to be scanned
     * @param op    operation to make on each chunk
     */
    public void forEachChunk(LongRange range, Consumer<RangedList<T>> op) {
        LongRange result = (range != null) ? range.findOverlap(chunks) : chunks.firstKey();
        while (true) {
            if (result == null) {
                break;
            }
            final LongRange inter = range.intersection(result);
            if (inter != null) {
                op.accept(chunks.get(result));
            }
            if (range != null && result.to >= range.to) {
                break;
            }
            result = chunks.higherKey(new LongRange(result.to - 1));
        }
    }

    @Deprecated
    private List<Future<?>> forEachParallelBody(ExecutorService pool, int nthreads, Consumer<ChunkedList<T>> run) {
        final List<ChunkedList<T>> separated = this.separate(nthreads);
        final List<Future<?>> futures = new ArrayList<>();
        for (final ChunkedList<T> sub : separated) {
            futures.add(pool.submit(() -> {
                run.accept(sub);
            }));
        }
        return futures;
    }

    private void forEachParallelBody(int parallelism, Consumer<ChunkedList<T>> run) {
        final List<ChunkedList<T>> separated = this.separate(parallelism);
        for (final ChunkedList<T> sub : separated) {
            async(() -> {
                run.accept(sub);
            });
        }
    }

    /**
     * Finds the chunk containing the provided index and returns the associated
     * value.
     *
     * @param i long index whose associated value should be returned
     * @return the value associated with the provided index
     * @throws IndexOutOfBoundsException if the provided index is not contained by
     *                                   any chunk in this instance
     * @see #containsIndex(long)
     */
    public T get(long i) {
        final LongRange r = new LongRange(i);
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
        if (entry == null || !entry.getKey().contains(i)) {
//            entry = chunks.ceilingEntry(r);
//            if (entry == null || !entry.getKey().contains(i)) {
            throw new IndexOutOfBoundsException("ChunkedList: index " + i + " is not within the range of any chunk");
//            }
        }
        final RangedList<T> chunk = entry.getValue();
        return chunk.get(i);
    }

    /**
     * Returns the chunk in this instance which contain the specified
     * {@link LongRange}.
     * <p>
     * The specified range needs to be fully included into a single chunk contained
     * in this instance, or exactly match the range of an existing chunk. Calling
     * this method with a {@link LongRange} which spans multiple chunks, or which is
     * not (even partially) included into any single chunk is undefined behavior
     * (the method may return an arbitrary chunk or throw a
     * {@link NullPointerException}).
     *
     * @param lr the targeted range
     * @return the chunk that contains the specified range
     */
    public RangedList<T> getChunk(LongRange lr) {
        return chunks.floorEntry(lr).getValue();
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        // code from JavaAPI doc of List
        for (final RangedList<?> c : chunks.values()) {
            hashCode = 31 * hashCode + (c == null ? 0 : c.hashCode());
        }
        return hashCode;
    }

    /**
     * Indicates if this instance does not contain any chunk
     *
     * @return {@code true} if this instance does not contain any chunk
     */
    public boolean isEmpty() {
        return size.get() == 0;
    }

    /**
     * Returns an iterator on the values contained by every chunk in this instance.
     *
     * @return an iterator on the elements contained in this {@link ChunkedList}
     */
    @Override
    public Iterator<T> iterator() {
        return new It<>(chunks);
    }

    /**
     * Creates a new {@link ChunkedList} by applying the provided map function in
     * parallel to every element of every {@link Chunk} contained by this instance.
     *
     * @param <S>      the type produced by the map function
     * @param pool     the executor service in charge of realizing the parallel
     *                 operation
     * @param nthreads the degree of parallelism allowed for this operation. The
     *                 {@link ChunkedList}'s Chunks will be split into the specified
     *                 number of portions that contain roughly same number of
     *                 indices.
     * @param func     the mapping function taking a T as parameter and returning a
     *                 S
     * @return a newly created ChunkedList which contains the result of mapping the
     *         elements of this instance
     */
    public <S> ChunkedList<S> map(ExecutorService pool, int nthreads, Function<? super T, ? extends S> func) {
        final ChunkedList<S> result = new ChunkedList<>();
        final List<Future<?>> futures = mapParallelBody(pool, nthreads, func, result);
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ParallelExecutionException("[ChunkedList] exception raised by worker threads.", e);
            }
        }
        return result;
    }

    /**
     * Creates a new {@link ChunkedList} by applying the provided map function to
     * every element of every {@link Chunk} contained by this instance.
     *
     * @param <S>  the type produced by the map function
     * @param func the mapping function taking a T as parameter and returning a S
     * @return a newly created ChunkedList which contains the result of mapping the
     *         elements of this instance
     */
    public <S> ChunkedList<S> map(Function<? super T, ? extends S> func) {
        final ChunkedList<S> result = new ChunkedList<>();
        forEachChunk((RangedList<T> c) -> {
            final RangedList<S> r = c.map(func);
            result.add(r);
        });
        return result;
    }

    private <S> List<Future<?>> mapParallelBody(ExecutorService pool, int nthreads,
            Function<? super T, ? extends S> func, ChunkedList<S> result) {
        forEachChunk((RangedList<T> c) -> {
            result.add(new Chunk<S>(c.getRange()));
        });
        final List<ChunkedList<T>> separatedIn = this.separate(nthreads);
        final List<ChunkedList<S>> separatedOut = result.separate(nthreads);
        final List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < nthreads; i++) {
            final int i0 = i;
            futures.add(pool.submit(() -> {
                final ChunkedList<T> from = separatedIn.get(i0);
                final ChunkedList<S> to = separatedOut.get(i0);
                from.mapTo(to, func);
                return null;
            }));
        }
        return futures;
    }

    private <S> void mapTo(ChunkedList<S> to, Function<? super T, ? extends S> func) {
        final Iterator<RangedList<T>> fromIter = chunks.values().iterator();
        final Iterator<RangedList<S>> toIter = to.chunks.values().iterator();
        while (fromIter.hasNext()) {
            assert (toIter.hasNext());
            final RangedList<T> fromChunk = fromIter.next();
            final RangedList<S> toChunk = toIter.next();
            toChunk.setupFrom(fromChunk, func);
        }
    }

    /**
     * Returns the number of chunks contained in this instance
     *
     * @return number of chunks in this instance
     */
    public int numChunks() {
        return chunks.size();
    }

    /**
     * Performs the provided action on each element of this collection in parallel
     * using the provided {@link ExecutorService} with the specified degree of
     * parallelism. This action may involve extracting some information of type U
     * from individual elements and placing these into the Consumer (second argument
     * of the lambda expression). This {@link Consumer} used in the lambda exression
     * is obtained from the provided {@link ParallelReceiver} which will receive all
     * the U instances produced during this method. This method returns when all the
     * elements in the collection have been treated.
     *
     * @param <U>     type of the information extracted from individual elements
     * @param action  action to perform on individual elements of this collection,
     *                potentially extracting some information of type U and giving
     *                it to the {@link Consumer}, the second argument of the action
     * @param toStore {@link ParallelReceiver} instance which provides the
     *                {@link Consumer}s of each thread that will process the
     *                elements of this library and receive all the U elements
     *                extracted from this collection
     */
    public <U> void parallelForEach(BiConsumer<? super T, Consumer<? super U>> action,
            final ParallelReceiver<? super U> toStore) {
        parallelForEach(defaultParallelism(), action, toStore);
    }

    /**
     * Performs the provided action on every eleement in the collection in parallel
     * using the apgas finish-async. Returns when all operations have finished.
     *
     * @param action to action to perform on element contained in this instance
     */
    public void parallelForEach(Consumer<? super T> action) {
        parallelForEach(defaultParallelism(), action);
    }

    public <U> void parallelForEach(int parallelism, BiConsumer<? super T, Consumer<? super U>> action,
            final ParallelReceiver<? super U> toStore) {
        finish(() -> {
            forEachParallelBody(parallelism, (ChunkedList<T> sub) -> {
                sub.forEach(action, toStore.getReceiver());
            });
        });
    }

    public void parallelForEach(int parallelism, Consumer<? super T> action) {
        finish(() -> {
            forEachParallelBody(parallelism, (ChunkedList<T> sub) -> {
                sub.forEach(action);
            });
        });
    }

    public void parallelForEach(int parallelism, LongTBiConsumer<? super T> action) {
        finish(() -> {
            forEachParallelBody(parallelism, (ChunkedList<T> sub) -> {
                sub.forEach(action);
            });
        });
    }

    /**
     * Performs the provided action on every (long) key and (T) value in the
     * collection in parallel using the apgas finish-async. Returns when all
     * operations have finished
     *
     * @param action to action to perform on each pair of ({@code long} key and (T)
     *               element contained in this instance
     */
    public void parallelForEach(LongTBiConsumer<? super T> action) {
        parallelForEach(defaultParallelism(), action);
    }

    /**
     * Return the ranges on which the chunks of this instance are defined in a
     * collection
     *
     * @return the {@link LongRange}s on which each {@link Chunk} contains in this
     *         instance are defined, in a collection
     */
    public Collection<LongRange> ranges() {
        return chunks.keySet();
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList}
     * using the reducer provided as parameter
     *
     * @param <R>     type of the reducer
     * @param reducer reducer to be used to reduce this parameter
     * @return the reducer provided as parameter after the reduction has completed
     */
    public <R extends Reducer<R, T>> R reduce(R reducer) {
        forEach(t -> reducer.reduce(t));
        return reducer;
    }

    /**
     * Sequentially reduces all the Chunks of Ts contained in this
     * {@link ChunkedList} into the provided reducer and returns that reducer.
     *
     * @param <R>     the type of the reducer
     * @param reducer the reducer into which this bag needs to be reduced
     * @return the reducer given as parameter after it has been applied to every
     *         list in this {@link Bag}
     */
    public <R extends Reducer<R, RangedList<T>>> R reduceChunk(R reducer) {
        forEachChunk(rl -> reducer.reduce(rl));
        return reducer;
    }

    /**
     * Removes and returns the chunk contained in this instance which is defined the
     * range provided as parameter. The specified range must match the exact bounds
     * of a chunk contained in this instance. If there are no chunks defined on the
     * specified range contained in this instance, returns null.
     *
     * @param range the range needs to be removed
     * @return the removed chunk, or null if there was no such chunk contained in
     *         this instance
     */
    public RangedList<T> remove(LongRange range) {
        final RangedList<T> removed = chunks.remove(range);
        if (removed != null) {
            size.addAndGet(-removed.size());
        }
        return removed;
    }

    /**
     * Removes and returns a chunk whose {@link LongRange} on which it is defined
     * matches the one on which the provided {@link RangedList} is defined.
     *
     * @param c the chunk whose matching range needs to be removed
     * @return the removed chunk, or null if there was no such chunk contained in
     *         this instance
     * @deprecated programmers should use method {@link #remove(LongRange)} instead
     */
    @Deprecated
    public RangedList<T> remove(RangedList<T> c) {
        return remove(c.getRange());
//        final RangedList<T> removed = chunks.remove(c.getRange());
//        if (removed != null) {
//            size.addAndGet(-removed.size());
//        }
//        return removed;
    }

    /**
     * Separates the contents of the ChunkedList in <em>n</em> parts. This can be
     * used to apply a forEach method in parallel using 'n' threads for instance.
     * The method returns <em>n</em> lists, each containing a {@link ChunkedList} of
     * <em>T</em>s.
     *
     * @param n the number of parts in which to split the ChunkedList
     * @return <em>n</em> {@link ChunkedList}s containing the same number of
     *         elements
     */
    public List<ChunkedList<T>> separate(int n) {
        final long totalNum = size();
        final long rem = totalNum % n;
        final long quo = totalNum / n;
        final List<ChunkedList<T>> result = new ArrayList<>(n);
        if (chunks.isEmpty()) {
            return result;
        }
        RangedList<T> c = chunks.firstEntry().getValue();
        long used = 0;

        for (int i = 0; i < n; i++) {
            final ChunkedList<T> r = new ChunkedList<>();
            result.add(r);
            long rest = quo + ((i < rem) ? 1 : 0);
            while (rest > 0) {
                final LongRange range = c.getRange();
                if (c.size() - used < rest) { // not enough
                    final long from = range.from + used;
                    if (from != range.to) {
                        r.add(c.subList(from, range.to));
                    }
                    rest -= c.size() - used;
                    used = 0;
                    // TODO should we use iterator instead ?
                    c = chunks.higherEntry(range).getValue();
                } else {
                    final long from = range.from + used;
                    final long to = from + rest;
                    if (from != to) {
                        r.add(c.subList(from, to));
                    }
                    used += rest;
                    rest = 0;
                }

            }
        }
        return result;
    }

    /**
     * Finds the matching chunk and sets the provided value at the specified index.
     *
     * @param i     the index at which the value should be set
     * @param value the value to set at the specified index
     * @return the former value stored at this index, {@code null} if there were no
     *         previous value or if the previous value was {@code null}
     */
    public T set(long i, T value) {
        final LongRange r = new LongRange(i);
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
        if (entry == null || !entry.getKey().contains(i)) {
//            entry = chunks.ceilingEntry(r);
//            if (entry == null || !entry.getKey().contains(i)) {
            throw new IndexOutOfBoundsException("ChunkedList: index " + i + " is not with the range of any chunk");
//            }
        }
        final RangedList<T> chunk = entry.getValue();
        return chunk.set(i, value);
    }

    /**
     * Return to total number of mappings contained in this instance, i.e. the sum
     * of the size of each individual {@link Chunk} this instance holds.
     *
     * @return size of this instance as a {@code long}
     */
    public long size() {
        return size.get();
    }

    /**
     * TODO : Still not sure if it works.
     */
    public ArrayList<RangedList<T>> splitChunks(LongRange range) {
        final ArrayList<RangedList<T>> chunksToRet = new ArrayList<>();
        // Two cases to handle here, whether the specified range fits into a single
        // existing chunk or whether it spans multiple chunks
        final Map.Entry<LongRange, RangedList<T>> lowSideEntry = chunks.floorEntry(range);
        if (lowSideEntry != null && lowSideEntry.getKey().from <= range.from && range.to <= lowSideEntry.getKey().to) {
            // The given range is included in (or identical) to an existing Chunk.
            // Only one Chunk needs to be split (if any).
            while (!attemptSplitChunkAtTwoPoints(range)) {
                ;
            }
            chunksToRet.add(chunks.get(range));
        } else {
            // The given range spans multiple ranges, the check on whether chunks need to be
            // split needs to be done separately on single points
            final LongRange leftSplit = new LongRange(range.from);
            final LongRange rightSplit = new LongRange(range.to);

            while (!attemptSplitChunkAtSinglePoint(leftSplit)) {
                ;
            }
            while (!attemptSplitChunkAtSinglePoint(rightSplit)) {
                ;
            }

            // Accumulate all the chunks that are spanned by the range specified as
            // parameter
            final NavigableSet<LongRange> keySet = chunks.keySet();
            LongRange rangeToAdd = keySet.ceiling(range);

            while (rangeToAdd != null && rangeToAdd.to <= range.to) {
                chunksToRet.add(chunks.get(rangeToAdd));
                rangeToAdd = keySet.higher(rangeToAdd);
            }
        }

        return chunksToRet;
    }

    /**
     * TODO : Still not sure if it works.
     */
    public ChunkedList<T> subList(LongRange range) {
        final ChunkedList<T> sub = new ChunkedList<>();
        LongRange result = range.findOverlap(chunks);
        while (true) {
            if (result == null) {
                break;
            }
            final LongRange inter = range.intersection(result);
            if (inter != null) {
                sub.add(new RangedListView<>(chunks.get(result), inter));
            }
            if (result.to >= range.to) {
                break;
            }
            result = chunks.higherKey(result);
        }
        return sub;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[ChunkedList(" + chunks.size() + ")");
        for (final RangedList<T> c : chunks.values()) {
            sb.append("," + c);
        }
        sb.append("]");
        return sb.toString();
    }

    private void waitNfutures(List<Future<?>> futures) {
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ParallelExecutionException("[ChunkedList] exception raised by worker threads.", e);
            }
        }
    }
}
