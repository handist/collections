/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import handist.collections.function.LongTBiConsumer;

/**
 * Large collection that can contain objects mapped to long indices. 
 *
 * @param <T> type of the elements handled by this instance
 */
public class Chunk<T> extends AbstractCollection<T> implements List<T>, RangedList<T>, Serializable {

	/**
	 * Iterator class for Chunk
	 * @param <T> type on which the iterator operates
	 */
	private static class It<T> implements ListIterator<T> {
		private Chunk<T> chunk;
		private int i; // offset inside the chunk

		public It(Chunk<T> chunk) {
			this.chunk = chunk;
			this.i = -1;
		}

		public It(Chunk<T> chunk, long i0) {
			if (!chunk.range.contains(i0)) {
				throw new IndexOutOfBoundsException();  
			}
			this.chunk = chunk;
			this.i = (int) (i0 - chunk.range.from - 1);
		}

		@Override
		public void add(T e) {
			throw new UnsupportedOperationException();
		}

		@Override
		public boolean hasNext() {
			return i + 1 < chunk.size();
		}

		@Override
		public boolean hasPrevious() {
			return i > 0;
		}

		@Override
		@SuppressWarnings("unchecked")
		public T next() {
			return (T) chunk.a[++i];
		}

		@Override
		public int nextIndex() {
			// TODO index may become long..
			throw new UnsupportedOperationException();
		}

		@Override @SuppressWarnings("unchecked")
		public T previous() {
			return (T) chunk.a[--i];
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

	}
	/** Serial Version UID */
	private static final long serialVersionUID = -7691832846457812518L;

	/** Array containing the T objects */
	private Object[] a;

	/** Range on which this instance is defined */
	private LongRange range;

	/**
	 * Builds a Chunk with the given range and no mapping.
	 * <p>
	 * The given LongRange should have a strictly positive size. Giving a 
	 * {@link LongRange} instance with identical lower and upper bounds will
	 * result in a {@link IllegalArgumentException} being thrown.
	 * <p>
	 * If the {@link LongRange} provided has a range that exceeds 
	 * {@value handist.collections.Config#maxChunkSize}, an {@link IllegalArgumentException} will be
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
	 * {@value handist.collections.Config#maxChunkSize}, an {@link IllegalArgumentException} will be
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

	/**
	 * Builds a {@link Chunk} with the provided {@link LongRange} with each long
	 * in the provided range mapped to object t. 
	 * The given LongRange should have a strictly positive size. Giving a 
	 * {@link LongRange} instance with identical lower and upper bounds will
	 * result in a {@link IllegalArgumentException} being thrown.
	 * <p>
	 * If the {@link LongRange} provided has a range that exceeds 
	 * {@value handist.collections.Config#maxChunkSize}, an {@link IllegalArgumentException} will be
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
     * Builds a {@link Chunk} with the provided {@link LongRange}. 
     * The provided initializer generates the initial value of the element for each index.
     * The given LongRange should have a strictly positive size. Giving a 
     * {@link LongRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds 
     * {@value handist.collections.Config#maxChunkSize}, an {@link IllegalArgumentException} will be
     * be thrown. 
     *   
     * @param range the range of the chunk to build
     * @param initializer generates the initial value of the element for each index.
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with
     *  the provided range. 
     */
    public Chunk(LongRange range, Function<Long, T> initializer) {
        this(range);
        range.forEach((long index)->{
           a[(int)(index-range.from)] = initializer.apply(index); 
        });
    }
	/**
	 * Not supported.
	 * Users should use method {@link #set(int, Object)} instead
	 * @throws UnsupportedOperationException systematically
	 */
	@Override
	public void add(int index, T element) {
		throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
	}

	/**
	 * Not supported.
	 * @throws UnsupportedOperationException systematically
	 */
	@Override
	public boolean addAll(int index, Collection<? extends T> c) {
		// TODO implement this method?
		throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
	}

	/**
	 * Not supported
	 * @throws UnsupportedOperationException systematically
	 */
	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}
	/**
	 * Returns a new Chunk defined on the same {@link LongRange} and with the 
	 * same contents as this instance. 
	 * @return a copy of this instance
	 */
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
	public boolean contains(Object v) {
		for (Object e : a) {
			if (v == null ? e == null : v.equals(e)) {
				return true;
			}
		}
		return false;
	}    

	@Override
	public boolean equals(Object o) {
		return RangedList.equals(this, o);
	}

	@Override
	public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
			Consumer<? super U> receiver) {
		rangeCheck(range);
		// IntStream.range(begin, end).forEach();
		for (long i = range.from; i < range.to; i++) {
			action.accept(get(i), receiver);
		}
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

	@Override
	public void forEach(LongRange range, final LongTBiConsumer<? super T> action) {
		rangeCheck(range);
		// IntStream.range(begin, end).forEach();
		for (long i = range.from; i < range.to; i++) {
			action.accept(i, get(i));
		}
	}

	@Override
	public T get(int index) {
		return get((long) index);
	}

	/**
	 * Get the element indexed by the {@code index}. 
	 * 
	 *  @throws IndexOutofBoundsException the given index is out of range.
	 *  
	 * @param index
	 * @return the element indexed by the {@code index}. 
	 */
	@Override
	public T get(long index) {
		if(!getRange().contains(index))
			throw new IndexOutOfBoundsException(rangeMsg(index));
		return getUnsafe(index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public LongRange getRange() {
		return range;
	}

	@SuppressWarnings("unchecked")
	final T getUnsafe(long index) {  // when range check was done
		long offset = index - range.from; 
		return (T) a[(int)offset];
	}

	@Override
	public int hashCode() {
		return RangedList.hashCode(this);
	}

	/**
	 * Not supported.
	 * @throws UnsupportedOperationException systematically
	 */
	@Override
	public int indexOf(Object o) {
		throw new UnsupportedOperationException("[Chunk] only support long index.");
	}

	@Override
	public Iterator<T> iterator() {
		return new It<T>(this);
	}

	@Override
	public Iterator<T> iteratorFrom(long i) {
		return new It<T>(this, i);
	}

	/**
	 * Not supported.
	 * @throws UnsupportedOperationException systematically
	 */
	@Override
	public int lastIndexOf(Object o) {
		throw new UnsupportedOperationException("[Chunk] only support long index.");
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
	public long longSize() {
		return range.to - range.from;
	}

	String rangeMsg(long index) {
		return "[Chunk] range "+ index + " is out of " + getRange();
	}

	String rangeMsg(LongRange range) {
		return "[Chunk] range "+ range + " is not contained in  " + getRange();
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		this.range = (LongRange) in.readObject();
		this.a = (Object[]) in.readObject();
		// System.out.println("readChunk:"+this);
	}

	@Override
	public T remove(int index) {
		throw new UnsupportedOperationException("[Chunk] does not support resize operation.");
	}

	@Override
	public T set(int index, T element) {
		return set((long) index, element);
	}

	/**
	 * Set the given value at the given index. 
	 * 
	 *  @throws IndexOutofBoundsException the given index is out of range.
	 *  
	 * @param index
	 * @return the previous value stored at the index.
	 */
	@Override
	public T set(long index, T value) {
		if(!getRange().contains(index))
			throw new IndexOutOfBoundsException(rangeMsg(index));            
		return setUnsafe(index,value);
	}

	@SuppressWarnings("unchecked")
	final T setUnsafe(long index, T v) { // when range check was done
		long offset  = index - range.from;
		T prev = (T) a[(int)offset];
		a[(int)offset] = v;
		return prev;
	}

	@Override
	public <S> void setupFrom(RangedList<S> from, Function<? super S, ? extends T> func) {
		rangeCheck(from.getRange());
		if (range.size() > Integer.MAX_VALUE)
			throw new Error("[Chunk] the size of RangedList cannot exceed Integer.MAX_VALUE.");
		LongTBiConsumer<S> consumer = (long index, S s) -> {
			T r = func.apply(s);
			a[(int) (index - range.from)] = r;
		};
		from.forEach(consumer);
	}

	/**
	 * Returns the number 
	 */
	@Override
	public int size() {
		return (int) longSize();
	}

	/**
	 * Not supported.
	 * @throws UnsupportedOperationException systematically
	 */
	@Override
	public List<T> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException("[Chunk] does not support copy operation.");
	}

	@Override
	public Object[] toArray() {
		return a;
	}

	@Override
	public Object[] toArray(LongRange newRange) {
		if(!range.contains(newRange)) {
			throw new IndexOutOfBoundsException(rangeMsg(newRange));
		}
		if (newRange.from == range.from && newRange.to == range.to) {
			return a;
		}
		if (newRange.from == newRange.to) {
			return new Object[0];
		}
		long newSize = (newRange.to - newRange.from);
		if (newSize > Config.maxChunkSize) {
			throw new IllegalArgumentException("[Chunk] the size of the result cannot exceed "+ Config.maxChunkSize);
		}
		Object[] newRail = new Object[(int) newSize];
		Arrays.fill(newRail, a[0]);
		System.arraycopy(a, (int) (newRange.from - range.from), newRail, 0, (int) newSize);
		return newRail;
	}

	@Override
	public Chunk<T> toChunk(LongRange newRange) {
		Object[] newRail = toArray(newRange);
		if (newRail == a) {
			return this;
		}
		if (newRail.length == 0) {
			throw new IllegalArgumentException("[Chunk] toChunk(emptyRange) is not permitted.");
		}
		return new Chunk<>(newRange, newRail);
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
