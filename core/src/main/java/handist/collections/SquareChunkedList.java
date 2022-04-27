package handist.collections;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;

import handist.collections.function.SquareIndexTConsumer;

public class SquareChunkedList<T> implements Serializable, Iterable<T> {

    private transient final ConcurrentSkipListMap<SquareRange, SquareChunk<T>> chunks;

    /**
     * Running tally of how many elements can be contained in the ChunkedList. It is
     * equal to the sum of the size of each individual chunk.
     */
    private transient final AtomicLong size;

    public SquareChunkedList() {
        chunks = new ConcurrentSkipListMap<>();
        size = new AtomicLong(0l);
    }

    SquareChunkedList(ConcurrentSkipListMap<SquareRange, SquareChunk<T>> chunks) {
        this.chunks = chunks;
        long accumulator = 0l;
        for (final SquareRange r : chunks.keySet()) {
            accumulator += r.size();
        }
        size = new AtomicLong(accumulator);
    }

    public void add(SquareRangedList<T> c) {
        // TODO
    }

    public void clear() {
        // TODO
    }

    @Override
    protected Object clone() {
        // TODO
        return null;
    }

    public boolean contains(Object o) {
        // TODO
        return false;
    }

    public boolean containsAll(Collection<?> c) {
        // TODO
        return false;
    }

    public boolean containsChunk(RangedList<T> c) {
        // TODO
        return false;
    }

    public boolean containsIndex(long i, long j) {
        // TODO
        return false;
    }

    public boolean containsRange(SquareRange range) {
        // TODO
        return false;
    }

    private int defaultParallelism() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    @Override
    public boolean equals(Object o) {
        // TODO
        return false;
    }

    public List<SquareRangedList<T>> filterChunk(Predicate<SquareRangedList<? super T>> filter) {
        // TODO
        return null;
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        // TODO
    }

    public void forEach(SquareIndexTConsumer<T> action) {
        // TODO
    }

    public void forEach(SquareRange subRange, Consumer<T> action) {
        // TODO
    }

    public void forEach(SquareRange subRange, SquareIndexTConsumer<T> action) {
        // TODO
    }

    public void forEachChunk(Consumer<RangedList<T>> op) {
        // TODO
    }

    public T get(long row, long column) {
        // TODO
        return null;
    }

    public SquareRangedList<T> getChunk(SquareRange sr) {
        // TODO
        return null;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        // code from JavaAPI doc of List
        for (final SquareRangedList<?> c : chunks.values()) {
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

    @Override
    public Iterator<T> iterator() {
        // TODO
        return null;
    }

    public void parallelForEach(Consumer<T> action) {
        // TODO
    }

    public void parallelForEach(SquareIndexTConsumer<T> action) {
        // TODO
    }

    public void parallelForEach(SquareRange subRange, Consumer<T> action) {
        // TODO
    }

    public void parallelForEach(SquareRange subRange, SquareIndexTConsumer<T> action) {
        // TODO
    }

    public Collection<SquareRange> ranges() {
        return chunks.keySet();
    }

    public T set(long row, long clolumn, T value) {
        // TODO
        return null;
    }

    public long size() {
        // TODO
        return 0l;
    }

    public SquareChunkedList<T> subList(SquareRange range) {
        // TODO
        return null;
    }

}
