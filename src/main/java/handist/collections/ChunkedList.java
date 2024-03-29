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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

    /** Serial Version UID */
    private static final long serialVersionUID = 6899796587031337979L;

    /**
     * Chunks contained by this instance. They are sorted using the
     * {@link LongRange} ordering.
     */
    private final ConcurrentSkipListMap<LongRange, RangedList<T>> chunks;

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
        chunks = new ConcurrentSkipListMap<>();
        size = new AtomicLong(0l);
    }

    /**
     * Constructor which takes an initial {@link ConcurrentSkipListMap} of
     * {@link LongRange} mapped to {@link RangedList}.
     *
     * @param chunks initial mappings of {@link LongRange} and {@link Chunk}s
     */
    public ChunkedList(ConcurrentSkipListMap<LongRange, RangedList<T>> chunks) {
        this.chunks = chunks;
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
            // TODO
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
        forEachParallelBody((ChunkedList<T> sub) -> {
            sub.forEach(action, toStore.getReceiver());
        });
    }

    public void asyncForEach(Consumer<? super T> action) {
        forEachParallelBody((ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
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

    public void asyncForEach(LongTBiConsumer<? super T> action) {
        forEachParallelBody((ChunkedList<T> sub) -> {
            sub.forEach(action);
        });
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
        final ConcurrentSkipListMap<LongRange, RangedList<T>> newChunks = new ConcurrentSkipListMap<>();
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
        return chunks.containsValue(c);
    }

    public boolean containsIndex(long i) {
        final LongRange r = new LongRange(i);
        Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
        if (entry == null || !entry.getKey().contains(i)) {
            entry = chunks.ceilingEntry(r);
            if (entry == null || !entry.getKey().contains(i)) {
                return false;
            }
        }
        return true;
    }

    public boolean containsRange(LongRange range) {
        return range.contained(chunks);
    }

    /*
     * public Map<LongRange, RangedList<T>> filterChunk0(Predicate<RangedList<?
     * super T>> filter) { ConcurrentSkipListMap<LongRange, RangedList<T>> map = new
     * ConcurrentSkipListMap<>(); for (RangedList<T> c : chunks.values()) { if
     * (filter.test(c)) { map.put(c.getRange(), c); } } return map; }
     */

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

    private void forEachParallelBody(Consumer<ChunkedList<T>> run) {
        final List<ChunkedList<T>> separated = this.separate(Runtime.getRuntime().availableProcessors() * 2);
        for (final ChunkedList<T> sub : separated) {
            async(() -> {
                run.accept(sub);
            });
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
        Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
        if (entry == null || !entry.getKey().contains(i)) {
            entry = chunks.ceilingEntry(r);
            if (entry == null || !entry.getKey().contains(i)) {
                throw new IndexOutOfBoundsException(
                        "ChunkedList: index " + i + " is not within the range of any chunk");
            }
        }
        final RangedList<T> chunk = entry.getValue();
        return chunk.get(i);
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
        finish(() -> {
            forEachParallelBody((ChunkedList<T> sub) -> {
                sub.forEach(action, toStore.getReceiver());
            });
        });
    }

    /**
     * Performs the provided action on every eleement in the collection in parallel
     * using the apgas finish-async. Returns when all operations have finished.
     *
     * @param action to action to perform on element contained in this instance
     */
    public void parallelForEach(Consumer<? super T> action) {
        finish(() -> {
            forEachParallelBody((ChunkedList<T> sub) -> {
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
        finish(() -> {
            forEachParallelBody((ChunkedList<T> sub) -> {
                sub.forEach(action);
            });
        });
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
     */
    @Deprecated
    public RangedList<T> remove(RangedList<T> c) {
        final RangedList<T> removed = chunks.remove(c.getRange());
        if (removed != null) {
            size.addAndGet(-removed.size());
        }
        return removed;
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
        Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
        if (entry == null || !entry.getKey().contains(i)) {
            entry = chunks.ceilingEntry(r);
            if (entry == null || !entry.getKey().contains(i)) {
                throw new IndexOutOfBoundsException("ChunkedList: index " + i + " is out of range of " + chunks);
            }
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
