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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


/**
 * Large collection that can contain objects mapped to long indices.
 *
 * @param <T> type of the elements handled by this instance
 */
public class Chunk<T> extends RangedList<T> implements Serializable, KryoSerializable {

    /**
     * ListIterator class for Chunk
     *
     * @param <T> type on which the iterator operates
     */
    protected static class ListIt<T> implements RangedListIterator<T> {
        private final Chunk<T> chunk;
        private int i; // offset inside the chunk
        private int head;
        private int limit;
        private int lastReturnedShift = -1;

        public ListIt(Chunk<T> chunk) {
            this.chunk = chunk;
            this.head = 0;
            this.limit = (int)chunk.size();
            this.i = -1;
        }
        public ListIt() {
            this.chunk = null;
            this.head = 0;
            this.limit = 0;
            this.i = -1;
        }
        private LongRange iterRange() {
            return new LongRange(chunk.getRange().from + head, chunk.getRange().from + limit);
        }



        public ListIt(Chunk<T> chunk, long i0) {
            if (!chunk.range.contains(i0)) {
                throw new IndexOutOfBoundsException();
            }
            this.chunk = chunk;
            this.head = 0;
            this.limit = (int)chunk.size();
            this.i = (int) (i0 - chunk.range.from - 1);
        }
        public ListIt(Chunk<T> chunk, LongRange range0, long i0) {
            range0 = chunk.getRange().intersection(range0);
            if(range0 == null) {
                throw new IndexOutOfBoundsException();
            }
            if (!range0.contains(i0)) {
                throw new IndexOutOfBoundsException();
            }
            this.chunk = chunk;
            this.head = (int)(range0.from-chunk.range.from);
            this.limit = (int)(range0.to-chunk.range.from);
            this.i = (int) (i0 - chunk.range.from - 1);
        }
        public ListIt(Chunk<T> chunk, LongRange range0) {
            range0 = chunk.getRange().intersection(range0);
            if(range0 == null) {
                throw new IndexOutOfBoundsException();
            }
            this.chunk = chunk;
            this.head = (int)(range0.from-chunk.range.from);
            this.limit = (int)(range0.to-chunk.range.from);
            this.i = (int) (range0.from - chunk.range.from - 1);
        }


        @Override
        public boolean hasNext() {
            return i + 1 < limit;
        }

        @Override
        public boolean hasPrevious() {
            return i > head;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            if(!hasNext())
                throw new IndexOutOfBoundsException("[Chunk.It] range mismatch: " + iterRange() + " does not include " + i);
            lastReturnedShift = 0;
            return (T) chunk.a[++i];
        }

        @Override
        public long nextIndex() {
            return chunk.range.from + i + 1;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T previous() {
            if(!hasPrevious())
                throw new IndexOutOfBoundsException("[Chunk.It] range mismatch: " + iterRange() + " does not include " + i);
            lastReturnedShift = 1;
            return (T) chunk.a[i--];
        }

        @Override
        public long previousIndex() {
            return chunk.range.from + i;
        }

        @Override
        public void set(T e) {
            if (lastReturnedShift == -1) {
                throw new IllegalStateException("[Chunk.It] Either method "
                        + "previous or next needs to be called before method set" + " can be used");
            }
            chunk.a[i + lastReturnedShift] = e; // FIXME THIS IS NOT CORRECT !!!
        }
    }

    /**
     * Iterator class for Chunk
     *
     * @param <T> type on which the iterator operates
     */
    protected static class It<T> implements Iterator<T> {
        private final Chunk<T> chunk;
        private int i; // offset inside the chunk
        private int limit;

        public It(Chunk<T> chunk) {
            this.chunk = chunk;
            this.limit = (int)chunk.size();
            this.i = -1;
        }
        public It() {
            this.chunk = null;
            this.limit = 0;
            this.i = -1;
        }
        public It(Chunk<T> chunk, LongRange range0) {
            range0 = chunk.getRange().intersection(range0);
            if(range0 == null) {
                throw new IndexOutOfBoundsException();
            }
            this.chunk = chunk;
            this.limit = (int)(range0.to-chunk.range.from);
            this.i = (int) (range0.from - chunk.range.from - 1);
        }
        @Override
        public boolean hasNext() {
            return i + 1 < limit;
        }

        @Override
        @SuppressWarnings("unchecked")
        public T next() {
            if(!hasNext()) throw new IndexOutOfBoundsException();
            return (T) chunk.a[++i];
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
     * {@link LongRange} instance with identical lower and upper bounds will result
     * in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds
     * {@value handist.collections.Config#maxChunkSize}, an
     * {@link IllegalArgumentException} will be be thrown.
     *
     * @param range the range of the chunk to build
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with the
     *                                  provided range.
     */
    public Chunk(LongRange range) {
        final long size = range.to - range.from;
        if (size > Config.maxChunkSize) {
            throw new IllegalArgumentException(
                    "The given range " + range + " exceeds the maximum Chunk size " + Config.maxChunkSize);
        } else if (size <= 0) {
            throw new IllegalArgumentException("Cannot build a Chunk with " + "LongRange " + range
                    + ", should have a strictly positive" + " size");
        }
        a = new Object[(int) size];
        this.range = range;
    }

    /**
     * Builds a {@link Chunk} with the provided {@link LongRange}. The provided
     * initializer generates the initial value of the element for each index. The
     * given LongRange should have a strictly positive size. Giving a
     * {@link LongRange} instance with identical lower and upper bounds will result
     * in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds
     * {@value handist.collections.Config#maxChunkSize}, an
     * {@link IllegalArgumentException} will be be thrown.
     *
     * @param range       the range of the chunk to build
     * @param initializer generates the initial value of the element for each index.
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with the
     *                                  provided range.
     */
    public Chunk(LongRange range, Function<Long, T> initializer) {
        this(range);
        range.forEach((long index) -> {
            a[(int) (index - range.from)] = initializer.apply(index);
        });
    }

    /**
     * Builds a {@link Chunk} with the provided {@link LongRange} and an initial
     * mapping for each long in the object array. The provided {@link LongRange} and
     * Object array should have the same size. An {@link IllegalArgumentException}
     * will be thrown otherwise.
     * <p>
     * The given {@link LongRange} should have a strictly positive size. Giving a
     * {@link LongRange} instance with identical lower and upper bounds will result
     * in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds
     * {@value handist.collections.Config#maxChunkSize}, an
     * {@link IllegalArgumentException} will be be thrown.
     *
     * @param range the range of the chunk to build
     * @param a     array with the initial mapping for every long in the provided
     *              range
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with the
     *                                  provided range and object array.
     */
    public Chunk(LongRange range, Object[] a) {
        this(range);
        if (a.length != range.size()) {
            throw new IllegalArgumentException("The length of the provided " + "array <" + a.length
                    + "> does not match the size of the " + "LongRange <" + range.size() + ">");
        }
        // TODO Do we check for objects in array a that are not of type T?
        // We can leave as is and let the code fail later in methods get and
        // others where a ClassCastException should be thrown.
        this.a = a;
    }

    /**
     * Builds a {@link Chunk} with the provided {@link LongRange} with each long in
     * the provided range mapped to object t. The given LongRange should have a
     * strictly positive size. Giving a {@link LongRange} instance with identical
     * lower and upper bounds will result in a {@link IllegalArgumentException}
     * being thrown.
     * <p>
     * If the {@link LongRange} provided has a range that exceeds
     * {@value handist.collections.Config#maxChunkSize}, an
     * {@link IllegalArgumentException} will be be thrown.
     *
     * @param range the range of the chunk to build
     * @param t     initial mapping for every long in the provided range
     * @throws IllegalArgumentException if a {@link Chunk} cannot be built with the
     *                                  provided range.
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
     * Returns a new Chunk defined on the same {@link LongRange} and with the same
     * contents as this instance.
     *
     * @return a copy of this instance
     */
    @Override
    public Chunk<T> clone() {
        // Object[] aClone = a.clone();
        final Object[] aClone = new Object[a.length];

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

        return new Chunk<>(this.range, aClone);
    }

    @Override
    public Chunk<T> cloneRange(LongRange newRange) {
        return range == newRange ? clone() : toChunk(newRange);
    }

    @Override
    public boolean contains(Object v) {
        for (final Object e : a) {
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

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(long index) {
        if (!getRange().contains(index)) {
            throw new IndexOutOfBoundsException(rangeMsg(index));
        }
        return getUnsafe(index);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public LongRange getRange() {
        return range;
    }

    /**
     * Returns the element located at the provided index. The provided index is
     * presumed valid and as such, no bound checking is done.
     *
     * @param index index whose value should be returned
     * @return the object stored at the provided index, possibly {@code null}
     */
    @SuppressWarnings("unchecked")
    final T getUnsafe(long index) { // when range check was done
        final long offset = index - range.from;
        return (T) a[(int) offset];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return RangedList.hashCode(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<T> iterator() {
        return new It<>(this);
    }

    /**
     * Creates and returns a new {@link RangedListIterator} on the elements
     * contained by this instance
     *
     * @return a new {@link RangedListIterator}
     */
    @Override
    public RangedListIterator<T> listIterator() {
        return new ListIt<>(this);
    }

    /**
     * Creates and returns a new {@link RangedListIterator} starting at the
     * specified index on the elements contained by this instance
     *
     * @param index the index of the first element to be returned by calling method
     *              {@link RangedListIterator#next()}
     * @return a new {@link RangedListIterator} starting at the specified index
     */
    public RangedListIterator<T> listIterator(long index) {
        return new ListIt<>(this, index);
    }

    private String rangeMsg(long index) {
        return "[Chunk] range " + index + " is out of " + getRange();
    }

    private String rangeMsg(LongRange range) {
        return "[Chunk] range " + range + " is not contained in " + getRange();
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this.range = (LongRange) kryo.readClassAndObject(input);
        this.a = (Object[]) kryo.readClassAndObject(input);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.range = (LongRange) in.readObject();
        this.a = (Object[]) in.readObject();
        // System.out.println("readChunk:"+this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T set(long index, T value) {
        if (!getRange().contains(index)) {
            throw new IndexOutOfBoundsException(rangeMsg(index));
        }
        return setUnsafe(index, value);
    }

    @SuppressWarnings("unchecked")
    private final T setUnsafe(long index, T v) { // when range check was done
        final long offset = index - range.from;
        final T prev = (T) a[(int) offset];
        a[(int) offset] = v;
        return prev;
    }

    @Override
    public Iterator<T> subIterator(LongRange range) {
        return new It<>(this, range);
    }
    @Override
    public RangedListIterator<T> subListIterator(LongRange range) {
        return new ListIt<>(this, range);
    }
    @Override
    public RangedListIterator<T> subListIterator(LongRange range, long i0) {
        return new ListIt<>(this, range, i0);
    }



    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray() {
        return a;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object[] toArray(LongRange newRange) {
        if (!range.contains(newRange)) {
            throw new IndexOutOfBoundsException(rangeMsg(newRange));
        }
        if (newRange.from == range.from && newRange.to == range.to) {
            return a;
        }
        if (newRange.from == newRange.to) {
            return new Object[0];
        }
        final long newSize = (newRange.to - newRange.from);
        if (newSize > Config.maxChunkSize) {
            throw new IllegalArgumentException("[Chunk] the size of the result cannot exceed " + Config.maxChunkSize);
        }
        final Object[] newRail = new Object[(int) newSize];
        Arrays.fill(newRail, a[0]);
        System.arraycopy(a, (int) (newRange.from - range.from), newRail, 0, (int) newSize);
        return newRail;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Chunk<T> toChunk(LongRange newRange) {
        final Object[] newRail = toArray(newRange);
        if (newRail == a) {
            return this;
        }
        if (newRail.length == 0) {
            throw new IllegalArgumentException("[Chunk] toChunk(emptyRange) is not permitted.");
        }
        return new Chunk<>(newRange, newRail);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<T> toList(LongRange r) {
        final ArrayList<T> list = new ArrayList<>((int) r.size());
        forEach(r, (t) -> list.add(t));
        return list;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        if (range == null) {
            return "[Chunk] in Construction";
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("[" + range + "]:");
        final long sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();

        for (long i = range.from, c = 0; i < range.to && c < sz; i++, c++) {
            if (c > 0) {
                sb.append(",");
            }
            sb.append("" + get(i));
            // if (c == sz) {
            // break;
            // }
        }
        if (sz < size()) {
            sb.append("...(omitted " + (size() - sz) + " elements)");
        }
        return sb.toString();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, range);
        kryo.writeClassAndObject(output, a);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        // System.out.println("writeChunk:"+this);
        out.writeObject(range);
        out.writeObject(a);
    }

    /*
     * public static void main(String[] args) { long i = 5; Chunk<Integer> c = new
     * Chunk<>(new LongRange(10 * i, 11 * i)); System.out.println("prepare: " + c);
     * IntStream.range(0, (int) i).forEach(j -> { int v = (int) (10 * i + j);
     * System.out.println("set@" + v); c.set(10 * i + j, v); });
     * System.out.println("Chunk :" + c);
     *
     * c.toArray(new LongRange(0, Config.maxChunkSize)); }
     */
}
