package handist.util;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class Chunk<T> extends AbstractCollection<T> implements RangedList<T> {

    private Object[] a;

    public LongRange range;

    public static <T> Chunk<T> make(Collection<T> c, LongRange range, T v) {
        Chunk<T> a = new Chunk<T>(range, v);
        a.addAll(c);
        return a;
    }

    public static <T> Chunk<T> make(Collection<T> c, LongRange range) {
        Chunk<T> a = new Chunk<T>(range);
        a.addAll(c);
        return a;
    }

    @Override
    public LongRange getRange() {
        return range;
    }

    @Override
    public boolean contains(Object v) {
        for (Object e : a) {
            if (v == null ? e == null : e.equals(v)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Chunk<T> clone() {
        // Object[] aClone = a.clone();
        Object[] aClone = new Object[a.length];

        //// FIXME: 2018/09/19 Need deep copy?
        // for (int i = 0; i < a.length; i++) {
        // try {
        // aClone[i] = ((Cloneable) a[i]).clone();
        // } catch (CloneNotSupportedException e) {
        // e.printStackTrace();
        // }
        // }

        Arrays.fill(aClone, a[0]);
        System.arraycopy(a, 0, aClone, 0, a.length);

        return new Chunk<T>(this.range, aClone);
    }

    @Override
    public Chunk<T> cloneRange(LongRange newRange) {
        return range == newRange ? clone() : toChunk(newRange);
    }

    @Override
    public Chunk<T> toChunk(LongRange newRange) {
        Object[] newRail = toArray(newRange);
        if (newRail == a) {
            return this;
        }
        return new Chunk<>(newRange, newRail);
    }

    @Override
    public RangedList<T> subList(long begin, long end) {
        long from = Math.max(begin, range.begin);
        long to = Math.min(end, range.end);
        if (from > to) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (begin == range.begin && end == range.end) {
            return this;
        }
        return new RangedListView<T>(this, new LongRange(begin, end));
    }

    @Override
    public T first() {
        return get(range.begin);
    }

    @Override
    public T last() {
        return get(range.end - 1);
    }

    @Override
    public T get(long i0) {
        int i = (int) (i0 - range.begin);
        return (T) a[i];
    }

    @Override
    public T set(long i0, T v) {
        int i = (int) (i0 - range.begin);
        // System.out.println("set (" + i0 + ", " + v + ") range.begin=" + range.begin +
        // ", i = " + i);
        T prev = (T) a[i];
        a[i] = v;
        return prev;
    }

    @Override
    public int size() {
        return (int) longSize();
    }

    @Override
    public long longSize() {
        return range.end - range.begin;
    }

    @Override
    public boolean isEmpty() {
        return a.length == 0;
    }

    @Override
    public Object[] toArray() {
        return a;
    }

    @Override
    public Object[] toArray(LongRange newRange) {
        long from = Math.max(range.begin, newRange.begin);
        long to = Math.min(range.end, newRange.end);
        if (from > to) {
            throw new ArrayIndexOutOfBoundsException(); // Need boundary check
        }
        if (from == range.begin && to == range.end) {
            return a;
        }
        if (from == to) {
            return new Object[0];
        }
        long newSize = (int) (newRange.end - newRange.begin);
        if (newSize > Config.maxChunkSize) {
            throw new IllegalArgumentException();
        }
        Object[] newRail = new Object[(int) newSize];
        Arrays.fill(newRail, a[0]);
        System.arraycopy(a, (int) (newRange.begin - range.begin), newRail, 0, (int) newSize);
        return newRail;
    }

    // Constructor

    public Chunk(LongRange range) {
        long size = range.end - range.begin;
        if (size > Config.maxChunkSize) {
            throw new IllegalArgumentException();
        }
        a = new Object[(int) size];
        this.range = range;
    }

    public Chunk(LongRange range, T v) {
        long size = range.end - range.begin;
        if (size > Config.maxChunkSize) {
            throw new IllegalArgumentException();
        }
        a = new Object[(int) size];
        Arrays.fill(a, v);
        this.range = range;
    }

    public Chunk() {
        // a = new Object[];
        this.range = new LongRange(0, 1);
    }

    public Chunk(LongRange range, Object[] a) {
        this.a = a;
        this.range = range;
    }

    // iterator
    private static class It<T> implements Iterator<T> {
        private int i; // offset inside the chunk
        private Chunk<T> chunk;

        public It(Chunk<T> chunk) {
            this.chunk = chunk;
            this.i = -1;
        }

        public It(Chunk<T> chunk, long i0) {
            this.chunk = chunk;
            this.i = (int) (i0 - chunk.range.begin - 1);
        }

        @Override
        public boolean hasNext() {
            return i + 1 < chunk.size();
        }

        @Override
        public T next() {
            return (T) chunk.a[++i];
        }

    }

    @Override
    public Iterator<T> iterator() {
        return new It<T>(this);
    }

    @Override
    public Iterator<T> iteratorFrom(long i) {
        return new It<T>(this, i);
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        forEach(this.range, action);
    }

    @Override
    public void forEach(LongRange range, final Consumer<? super T> action) {
        long from = Math.max(range.begin, this.range.begin);
        long to = Math.min(range.end, this.range.end);
        if (from > to) {
            throw new ArrayIndexOutOfBoundsException(); // Need boundary check.
        }
        // IntStream.range(begin, end).forEach();
        for (long i = from; i < to; i++) {
            action.accept(get(i));
        }
    }

    public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, Consumer<? super U> receiver) {
        forEach(this.range, action, receiver);
    }

    public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
            Consumer<? super U> receiver) {
        long from = Math.max(range.begin, this.range.begin);
        long to = Math.min(range.end, this.range.end);
        if (from > to) {
            throw new ArrayIndexOutOfBoundsException(); // Need boundary check.
        }
        // IntStream.range(begin, end).forEach();
        for (long i = from; i < to; i++) {
            action.accept(get(i), receiver);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[" + range + "]");
        int sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();
        long c = 0;
        for (long i = range.begin; i < range.end; i++) {
            if (c++ > 0) {
                sb.append(",");
            }
            sb.append("" + get(i));
            if (c == sz) {
                break;
            }
        }
        if (sz < size()) {
            sb.append("...(omitted " + (size() - sz) + " elements)");
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        long i = 5;
        Chunk<Integer> c = new Chunk<>(new LongRange(10 * i, 11 * i));
        System.out.println("prepare: " + c);
        IntStream.range(0, (int) i).forEach(j -> {
            int v = (int) (10 * i + j);
            System.out.println("set@" + v);
            c.set(10 * i + j, v);
        });
        System.out.println("Chunk :" + c);
    }




}
