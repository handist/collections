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

import java.io.*;
import handist.collections.function.LongTBiConsumer;

public class Chunk<T> extends AbstractCollection<T> implements RangedList<T>, Serializable, List<T> {

    /** Serial Version UID */
	private static final long serialVersionUID = -7691832846457812518L;

	private Object[] a;

    public LongRange range;


    @Override
    public LongRange getRange() {
        return range;
    }

    @Override
    public boolean contains(Object v) {
        for (Object e : a) {
        	if (v == null ? e == null : v.equals(e)) {
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
        if (newRail.length == 0) {
        	throw new ArrayIndexOutOfBoundsException();
        }
        return new Chunk<>(newRange, newRail);
    }

    @Override
    public RangedList<T> subList(long begin, long end) {
    	if(begin > end) {
        	throw new IllegalArgumentException("Cannot obtain a sublist from " +
        			begin + " to " + end);
        }
    	if(begin < range.from || range.to < end) {
    		throw new IllegalArgumentException();
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
        if(!range.contains(newRange)) {
        	throw new ArrayIndexOutOfBoundsException();
        }
        if (newRange.from == range.from && newRange.to == range.to) {
            return a;
        }
        if (newRange.from == newRange.to) {
            return new Object[0];
        }
        long newSize = (newRange.to - newRange.from);
        if (newSize > Config.maxChunkSize) {
            throw new IllegalArgumentException();
        }
        Object[] newRail = new Object[(int) newSize];
        Arrays.fill(newRail, a[0]);
        System.arraycopy(a, (int) (newRange.from - range.from), newRail, 0, (int) newSize);
        return newRail;
    }

    /**
     * Builds a Chunk with the given range and no mapping.
     * <p>
     * The given LongRange should have a strictly positive size. Giving a 
     * {@link LongRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds 
     * {@value Config#maxChunkSize}, an {@link IllegalArgumentException} will be
     * be thrown. 
     *   
     * @param range the range of the chunk to build
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with
     * 	the provided range. 
     */
    public Chunk(LongRange range) {
        long size = range.to - range.from;
        if (size > Config.maxChunkSize) {
            throw new IllegalArgumentException("The given range " + range + 
            		" exceeds the maximum Chunk size " + Config.maxChunkSize);
        } else if (size <= 0) {
        	throw new IllegalArgumentException("Cannot build a Chunk with "
        			+ "LongRange " + range + ", should have a strictly positive"
        			+ " size");
        }
        a = new Object[(int) size];
        this.range = range;
    }

    /**
     * Builds a {@link Chunk} with the provided {@link LongRange} with each long
     * in the provided range mapped to object t. 
     * The given LongRange should have a strictly positive size. Giving a 
     * {@link LongRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds 
     * {@value Config#maxChunkSize}, an {@link IllegalArgumentException} will be
     * be thrown. 
     *   
     * @param range the range of the chunk to build
     * @param t initial mapping for every long in the provided range
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with
     * 	the provided range. 
     */
    public Chunk(LongRange range, T t) {
    	this(range);
    	// TODO Is this what we really want to do?
    	// The mapping will be on the SAME OBJECT for every long in LongRange.
    	// Don't we need a Generator<T> generator as argument and create an 
    	// instance for each key with Arrays.setAll(a, generator) ?
        Arrays.fill(a, t); 
        
    }

    /**
     * Builds a {@link Chunk} with the provided {@link LongRange} and an initial
     * mapping for each long in the object array. The provided {@link LongRange}
     * and Object array should have the same size. An 
     * {@link IllegalArgumentException} will be thrown otherwise.  
     * <p>
     * The given {@link LongRange} should have a strictly positive size. Giving a 
     * {@link LongRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds 
     * {@value Config#maxChunkSize}, an {@link IllegalArgumentException} will be
     * be thrown. 
     *   
     * @param range the range of the chunk to build
     * @param a array with the initial mapping for every long in the provided range
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with
     * 	the provided range and object array. 
     */
    public Chunk(LongRange range, Object[] a) {
    	this(range);
    	if (a.length != range.size()) {
    		throw new IllegalArgumentException("The length of the provided "
    				+ "array <" + a.length +"> does not match the size of the "
    				+ "LongRange <" + range.size() + ">");
    	}
    	// TODO Do we check for objects in array a that are not of type T?
    	// We can leave as is and let the code fail later in methods get and 
    	// others where a ClassCastException should be thrown.  
        this.a = a;
    }

    public <S> void setupFrom(RangedList<S> from, Function<? super S, ? extends T> func) {
        rangeCheck(from.getRange());
        if (range.size() > Integer.MAX_VALUE)
            throw new RuntimeException("[Chunk] number of elements cannot exceed Integer.MAX_VALUE.");
        LongTBiConsumer<S> consumer = (long index, S s) -> {
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

    public void forEach(LongRange range, final LongTBiConsumer<? super T> action) {
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
        sb.append("[" + range + "]:");
        int sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();
        
        for (long i = range.from, c = 0; i < range.to && c < sz; i++, c++) {
            if (c > 0) {
                sb.append(",");
            }
            sb.append("" + get(i));
//            if (c == sz) {
//                break;
//            }
        }
        if (sz < size()) {
            sb.append("...(omitted " + (size() - sz) + " elements)");
        }
        return sb.toString();
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
    
    /*
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
        
        c.toArray(new LongRange(0, Config.maxChunkSize));
    }
    */
}
