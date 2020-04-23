package handist.collections;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import java.io.*;
import handist.collections.function.LTConsumer;

public class Chunk<T> extends AbstractCollection<T> implements RangedList<T>, Serializable, List<T> {

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
        long from = Math.max(begin, range.from);
        long to = Math.min(end, range.to);
        if (from > to) {
            throw new ArrayIndexOutOfBoundsException();
        }
        if (begin == range.from && end == range.to) {
            return this;
        }
        return new RangedListView<T>(this, new LongRange(begin, end));
    }

    @Override
    @SuppressWarnings("unchecked")
    public T get(long i0) {
        int i = (int) (i0 - range.from);
        return (T) a[i];
    }

    @Override
    public T set(long i0, T v) {
        int i = (int) (i0 - range.from);
        // System.out.println("set (" + i0 + ", " + v + ") range.begin=" + range.begin +
        // ", i = " + i);
        @SuppressWarnings("unchecked")
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
        if (range == null)
            throw new Error("hxcdskcs");
        return range.to - range.from;
    }

    @Override
    public Object[] toArray() {
        return a;
    }

    @Override
    public Object[] toArray(LongRange newRange) {
        long from = Math.max(range.from, newRange.from);
        long to = Math.min(range.to, newRange.to);
        if (from > to) {
            throw new ArrayIndexOutOfBoundsException(); // Need boundary check
        }
        if (from == range.from && to == range.to) {
            return a;
        }
        if (from == to) {
            return new Object[0];
        }
        long newSize = (int) (newRange.to - newRange.from);
        if (newSize > Config.maxChunkSize) {
            throw new IllegalArgumentException();
        }
        Object[] newRail = new Object[(int) newSize];
        Arrays.fill(newRail, a[0]);
        System.arraycopy(a, (int) (newRange.from - range.from), newRail, 0, (int) newSize);
        return newRail;
    }

    // Constructor

    public Chunk(LongRange range) {
        long size = range.to - range.from;
        if (size > Config.maxChunkSize) {
            throw new IllegalArgumentException();
        }
        a = new Object[(int) size];
        this.range = range;
    }

    public Chunk(LongRange range, T v) {
        long size = range.to - range.from;
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
        a = new Object[1];
    }

    public Chunk(LongRange range, Object[] a) {
        if (range == null)
            throw new Error("This should not happen!");
        this.a = a;
        this.range = range;
    }

    public <S> void setupFrom(RangedList<S> from, Function<? super S, ? extends T> func) {
        rangeCheck(from.getRange());
        if (range.size() > Integer.MAX_VALUE)
            throw new RuntimeException("[Chunk] number of elements cannot exceed Integer.MAX_VALUE.");
        LTConsumer<S> consumer = (long index, S s) -> {
            T r = func.apply(s);
            a[(int) (index - range.from)] = r;
        };
        from.forEach(range, consumer);
    }

    // iterator
    private static class It<T> implements ListIterator<T> {
        private int i; // offset inside the chunk
        private Chunk<T> chunk;

        public It(Chunk<T> chunk) {
            this.chunk = chunk;
            this.i = -1;
        }

        public It(Chunk<T> chunk, long i0) {
            if (!chunk.range.contains(i0)) {
                throw new ArrayIndexOutOfBoundsException();  
            }
            this.chunk = chunk;
            this.i = (int) (i0 - chunk.range.from - 1);
        }

        @Override
        public boolean hasNext() {
            return i + 1 < chunk.size();
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            return (T) chunk.a[++i];
        }

        @Override
        public boolean hasPrevious() {
            return i > 0;
        }

        @Override @SuppressWarnings("unchecked")
        public T previous() {
            return (T) chunk.a[--i];
        }

        @Override
        public int nextIndex() {
            // TODO index may become long..
            throw new UnsupportedOperationException();
        }

        @Override
        public int previousIndex() {
            // TODO index may become long..
            throw new UnsupportedOperationException();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void set(T e) {
            chunk.a[i] = e;
        }

        @Override
        public void add(T e) {
            throw new UnsupportedOperationException();
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
    public void forEach(LongRange range, final Consumer<? super T> action) {
        rangeCheck(range);
        long from = range.from;
        long to = range.to;

        for (long i = from; i < to; i++) {
            action.accept(get(i));
        }
    }

    public void forEach(LongRange range, final LTConsumer<? super T> action) {
        rangeCheck(range);
        // IntStream.range(begin, end).forEach();
        for (long i = range.from; i < range.to; i++) {
            action.accept(i, get(i));
        }
    }

    public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
            Consumer<? super U> receiver) {
        rangeCheck(range);
        // IntStream.range(begin, end).forEach();
        for (long i = range.from; i < range.to; i++) {
            action.accept(get(i), receiver);
        }
    }

    @Override
    public String toString() {
        if (range == null) {
            return "[Chunk] in Construction";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("[" + range + "]");
        int sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();
        long c = 0;
        for (long i = range.from; i < range.to; i++) {
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

    private void writeObject(ObjectOutputStream out) throws IOException {
        // System.out.println("writeChunk:"+this);
        out.writeObject(range);
        out.writeObject(a);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.range = (LongRange) in.readObject();
        this.a = (Object[]) in.readObject();
        // System.out.println("readChunk:"+this);
    }

    // TODO ...
    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
    }

    @Override
    public T get(int index) {
        return get((long) index);
    }

    @Override
    public T set(int index, T element) {
        return set((long) index, element);
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
    }

    @Override
    public int indexOf(Object o) {
        throw new UnsupportedOperationException("[Chunk] only support long index.");
    }

    @Override
    public int lastIndexOf(Object o) {
        throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
    }

    @Override
    public ListIterator<T> listIterator() {
        return new It<T>(this);
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return new It<T>(this, (long)index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("[Chunk] does not support copy operation.");
    }
}
