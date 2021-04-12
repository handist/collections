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

import handist.collections.dist.util.Pair;

import java.io.Serializable;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;



public class SquareChunk<T> /* extends SquareRangedList<T>*/ implements Serializable /*, KryoSerializable*/ {

    /** Array containing the T objects */
    private final Object[] a;

    /** Range on which this instance is defined */
    private final SquareRange range;
    private final long innerSize;

//    /**
//     * Builds a Chunk with the given range and no mapping.
//     * <p>
//     * The given LongRange should have a strictly positive size. Giving a
//     * {@link LongRange} instance with identical lower and upper bounds will result
//     * in a {@link IllegalArgumentException} being thrown.
//     * <p>
//     * If the {@link LongRange} provided has a range that exceeds
//     * {@value Config#maxChunkSize}, an
//     * {@link IllegalArgumentException} will be be thrown.
//     *
//     * @param range the range of the chunk to build
//     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built with the
//     *                                  provided range.
//     */
    public SquareChunk(SquareRange range) {
        long outerSize = range.outer.size();
        this.innerSize = range.inner.size();
        long size = outerSize * innerSize;
        // FIXME
        if(size > Integer.MAX_VALUE) throw new RuntimeException("array size overflow");
        a = new Object[(int) size];
        this.range = range;
    }

//    /**
//     * Builds a {@link SquareChunk} with the provided {@link LongRange}. The provided
//     * initializer generates the initial value of the element for each index. The
//     * given LongRange should have a strictly positive size. Giving a
//     * {@link LongRange} instance with identical lower and upper bounds will result
//     * in a {@link IllegalArgumentException} being thrown.
//     * <p>
//     * If the {@link LongRange} provided has a range that exceeds
//     * {@value Config#maxChunkSize}, an
//     * {@link IllegalArgumentException} will be be thrown.
//     *
//     * @param range       the range of the chunk to build
//     * @param initializer generates the initial value of the element for each index.
//     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built with the
//     *                                  provided range.
//     */
    public SquareChunk(SquareRange range, BiFunction<Long, Long, T> initializer) {
        this(range);
        range.forEach((Long index, Long index2) -> {
            set(index, index2, initializer.apply(index, index2));
        });
    }

//    /**
//     * Builds a {@link SquareChunk} with the provided {@link LongRange} and an initial
//     * mapping for each long in the object array. The provided {@link LongRange} and
//     * Object array should have the same size. An {@link IllegalArgumentException}
//     * will be thrown otherwise.
//     * <p>
//     * The given {@link LongRange} should have a strictly positive size. Giving a
//     * {@link LongRange} instance with identical lower and upper bounds will result
//     * in a {@link IllegalArgumentException} being thrown.
//     * <p>
//     * If the {@link LongRange} provided has a range that exceeds
//     * {@value Config#maxChunkSize}, an
//     * {@link IllegalArgumentException} will be be thrown.
//     *
//     * @param range the range of the chunk to build
//     * @param a     array with the initial mapping for every long in the provided
//     *              range
//     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built with the
//     *                                  provided range and object array.
//     */
//    public SquareChunk(LongRange range, Object[] a) {
//        this(range);
//        if (a.length != range.size()) {
//            throw new IllegalArgumentException("The length of the provided " + "array <" + a.length
//                    + "> does not match the size of the " + "LongRange <" + range.size() + ">");
//        }
//        // TODO Do we check for objects in array a that are not of type T?
//        // We can leave as is and let the code fail later in methods get and
//        // others where a ClassCastException should be thrown.
//        this.a = a;
//    }

//    /**
//     * Builds a {@link SquareChunk} with the provided {@link LongRange} with each long in
//     * the provided range mapped to object t. The given LongRange should have a
//     * strictly positive size. Giving a {@link LongRange} instance with identical
//     * lower and upper bounds will result in a {@link IllegalArgumentException}
//     * being thrown.
//     * <p>
//     * If the {@link LongRange} provided has a range that exceeds
//     * {@value Config#maxChunkSize}, an
//     * {@link IllegalArgumentException} will be be thrown.
//     *
//     * @param range the range of the chunk to build
//     * @param t     initial mapping for every long in the provided range
//     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built with the
//     *                                  provided range.
//     */
//    public SquareChunk(LongRange range, T t) {
//        this(range);
//        // TODO Is this what we really want to do?
//        // The mapping will be on the SAME OBJECT for every long in LongRange.
//        // Don't we need a Generator<T> generator as argument and create an
//        // instance for each key with Arrays.setAll(a, generator) ?
//        Arrays.fill(a, t);
//    }

//    /**
//     * Returns a new Chunk defined on the same {@link LongRange} and with the same
//     * contents as this instance.
//     *
//     * @return a copy of this instance
//     */
//    @Override
//    public SquareChunk<T> clone() {
//        // Object[] aClone = a.clone();
//        final Object[] aClone = new Object[a.length];
//
//        //// FIXME: 2018/09/19 Need deep copy?
//        // for (int i = 0; i < a.length; i++) {
//        // try {
//        // aClone[i] = ((Cloneable) a[i]).clone();
//        // } catch (CloneNotSupportedException e) {
//        // e.printStackTrace();
//        // }
//        // }
//
//        Arrays.fill(aClone, a[0]);
//        System.arraycopy(a, 0, aClone, 0, a.length);
//
//        return new SquareChunk<>(this.range, aClone);
//    }

//    @Override
//    public SquareChunk<T> cloneRange(LongRange newRange) {
//        return range == newRange ? clone() : toChunk(newRange);
//    }
//
//    @Override
//    public boolean contains(Object v) {
//        for (final Object e : a) {
//            if (v == null ? e == null : v.equals(e)) {
//                return true;
//            }
//        }
//        return false;
//    }

//    @Override
//    public boolean equals(Object o) {
//        return RangedList.equals(this, o);
//    }

//    @Override
//    public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
//            Consumer<? super U> receiver) {
//        rangeCheck(range);
//        // IntStream.range(begin, end).forEach();
//        for (long i = range.from; i < range.to; i++) {
//            action.accept(get(i), receiver);
//        }
//    }
//
//    @Override
//    public void forEach(LongRange range, final Consumer<? super T> action) {
//        rangeCheck(range);
//        final long from = range.from;
//        final long to = range.to;
//
//        for (long i = from; i < to; i++) {
//            action.accept(get(i));
//        }
//    }

//    @Override
//    public void forEach(LongRange range, final LongTBiConsumer<? super T> action) {
//        rangeCheck(range);
//        // IntStream.range(begin, end).forEach();
//        for (long i = range.from; i < range.to; i++) {
//            action.accept(i, get(i));
//        }
//    }

    static class MySquareSiblingAccessor<T> implements SquareSiblingAccessor<T> {
        int offset;
        int innerSize;
        Object[] a;

        public MySquareSiblingAccessor(int offset, int innerSize, Object[] a) {
            this.offset = offset;
            this.innerSize = innerSize;
            this.a = a;
        }

        @Override
        public T get(int x, int y) {
            assert(x<=1 && x>=-1);
            assert(y<=1 && y>=-1);
            return (T)a[offset + x*innerSize + y];
        }
        @Override
        public void put(T v) {
            a[offset] = v;
        }
    }

    public void forEach(SquareRange range, final Consumer<SquareSiblingAccessor<T>> action) {
        // TODO
        // rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        long index1 = range.outer.from;
        long index2 = range.inner.from;
        long offset1 = index1 - getRange().outer.from;
        long offset2 = index2 - getRange().inner.from;
        //TODO overflow assert
        int offset = (int) (offset1 * innerSize + offset2);
        while (true) {
            // TODO overflow assert
            System.out.println("index:"+ index1 +","+index2 + ", offset" + offset1 + ","+offset2 + ","+offset);
            SquareSiblingAccessor<T> acc = new MySquareSiblingAccessor<>(offset, (int) innerSize, a);
            action.accept(acc);
            index2++;
            offset++;
            if(index2==range.inner.to) {
                index2 = range.inner.from;
                index1++;
                if(index1==range.outer.to) return;
                offset1++;
                //TODO overflow assert
                offset = (int)(offset1 * innerSize + offset2);
            }
        }
    }



    public void forEach(final BiConsumer<Pair<Long,Long>, ? super T> action) {
        // TODO
        // rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        long index = range.outer.from;
        long index2 = range.inner.from;
        int offset = 0;
        while (true) {
            T v = (T)a[offset];
            action.accept(new Pair<>(index, index2), v);
            index2++;
            offset++;
            if(index2==range.inner.to) {
                index2 = range.inner.from;
                index++;
                if(index==range.outer.to) return;
            }
        }
    }
    public void forEach(final Consumer<? super T> action) {
        // TODO
        // rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        for (Object o: a) {
            action.accept((T)o);
        }
    }



    /**
     * {@inheritDoc}
     */
    public T get(long index, long index2) {
        if (!getRange().outer.contains(index)) {
            throw new IndexOutOfBoundsException(/*rangeMsg(index)*/ index + " is outof "+getRange().outer);
        }
        if (!getRange().inner.contains(index2)) {
            throw new IndexOutOfBoundsException(/*rangeMsg(index2)*/ index2  + " is outof " + getRange().inner);
        }
        return getUnsafe(index, index2);
    }


    public SquareRange getRange() {
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
    final T getUnsafe(long index, long index2) { // when range check was done
        final long offset1 = index - range.outer.from;
        final long offset2 = index2 - range.inner.from;
        return (T) a[(int) (offset1 * innerSize + offset2)];
    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public int hashCode() {
//        return RangedList.hashCode(this);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public Iterator<T> iterator() {
//        return new It<>(this);
//    }

//    /**
//     * Creates and returns a new {@link RangedListIterator} on the elements
//     * contained by this instance
//     *
//     * @return a new {@link RangedListIterator}
//     */
//    public RangedListIterator<T> rangedListIterator() {
//        return new It<>(this);
//    }
//
//    /**
//     * Creates and returns a new {@link RangedListIterator} starting at the
//     * specified index on the elements contained by this instance
//     *
//     * @param index the index of the first element to be returned by calling method
//     *              {@link RangedListIterator#next()}
//     * @return a new {@link RangedListIterator} starting at the specified index
//     */
//    public RangedListIterator<T> rangedListIterator(long index) {
//        return new It<>(this, index);
//    }

//    private String rangeMsg(long index) {
//        return "[Chunk] range " + index + " is out of " + getRange();
//    }
//
//    private String rangeMsg(LongRange range) {
//        return "[Chunk] range " + range + " is not contained in " + getRange();
//    }

//    @Override
//    public void read(Kryo kryo, Input input) {
//        this.range = (LongRange) kryo.readClassAndObject(input);
//        this.a = (Object[]) kryo.readClassAndObject(input);
//    }
//
//    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
//        this.range = (LongRange) in.readObject();
//        this.a = (Object[]) in.readObject();
//        // System.out.println("readChunk:"+this);
//    }

    /**
     * {@inheritDoc}
     */
    public T set(long index, long index2, T value) {
        if (!getRange().outer.contains(index)) {
            throw new IndexOutOfBoundsException(/*rangeMsg(index)*/ index + " is outof "+getRange().outer);
        }
        if (!getRange().inner.contains(index2)) {
            throw new IndexOutOfBoundsException(/*rangeMsg(index2)*/ index2  + " is outof " + getRange().inner);
        }
        return setUnsafe(index, index2, value);
    }

    @SuppressWarnings("unchecked")
    private final T setUnsafe(long index, long index2, T v) { // when range check was done
        final long offset1 = index - range.outer.from;
        final long offset2 = index2 - range.inner.from;
        final long offset = offset1 * innerSize + offset2;
        final T prev = (T) a[(int) offset];
        a[(int) offset] = v;
        return prev;
    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public <S> void setupFrom(RangedList<S> from, Function<? super S, ? extends T> func) {
//        rangeCheck(from.getRange());
//        if (range.size() > Integer.MAX_VALUE) {
//            throw new Error("[Chunk] the size of RangedList cannot exceed Integer.MAX_VALUE.");
//        }
//        final LongTBiConsumer<S> consumer = (long index, S s) -> {
//            final T r = func.apply(s);
//            a[(int) (index - range.from)] = r;
//        };
//        from.forEach(consumer);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    public Object[] toArray() {
//        return a;
//    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public Object[] toArray(LongRange newRange) {
//        if (!range.contains(newRange)) {
//            throw new IndexOutOfBoundsException(rangeMsg(newRange));
//        }
//        if (newRange.from == range.from && newRange.to == range.to) {
//            return a;
//        }
//        if (newRange.from == newRange.to) {
//            return new Object[0];
//        }
//        final long newSize = (newRange.to - newRange.from);
//        if (newSize > Config.maxChunkSize) {
//            throw new IllegalArgumentException("[Chunk] the size of the result cannot exceed " + Config.maxChunkSize);
//        }
//        final Object[] newRail = new Object[(int) newSize];
//        Arrays.fill(newRail, a[0]);
//        System.arraycopy(a, (int) (newRange.from - range.from), newRail, 0, (int) newSize);
//        return newRail;
//    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public SquareChunk<T> toChunk(LongRange newRange) {
//        final Object[] newRail = toArray(newRange);
//        if (newRail == a) {
//            return this;
//        }
//        if (newRail.length == 0) {
//            throw new IllegalArgumentException("[Chunk] toChunk(emptyRange) is not permitted.");
//        }
//        return new SquareChunk<>(newRange, newRail);
//    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public List<T> toList(LongRange r) {
//        final ArrayList<T> list = new ArrayList<>((int) r.size());
//        forEach(r, (t) -> list.add(t));
//        return list;
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public String toString() {
//        if (range == null) {
//            return "[Chunk] in Construction";
//        }
//        final StringBuilder sb = new StringBuilder();
//        sb.append("[" + range + "]:");
//        final long sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();
//
//        for (long i = range.from, c = 0; i < range.to && c < sz; i++, c++) {
//            if (c > 0) {
//                sb.append(",");
//            }
//            sb.append("" + get(i));
//            // if (c == sz) {
//            // break;
//            // }
//        }
//        if (sz < size()) {
//            sb.append("...(omitted " + (size() - sz) + " elements)");
//        }
//        return sb.toString();
//    }

//    @Override
//    public void write(Kryo kryo, Output output) {
//        kryo.writeClassAndObject(output, range);
//        kryo.writeClassAndObject(output, a);
//    }
//
//    private void writeObject(ObjectOutputStream out) throws IOException {
//        // System.out.println("writeChunk:"+this);
//        out.writeObject(range);
//        out.writeObject(a);
//    }

    /*
     * public static void main(String[] args) { long i = 5; Chunk<Integer> c = new
     * Chunk<>(new LongRange(10 * i, 11 * i)); System.out.println("prepare: " + c);
     * IntStream.range(0, (int) i).forEach(j -> { int v = (int) (10 * i + j);
     * System.out.println("set@" + v); c.set(10 * i + j, v); });
     * System.out.println("Chunk :" + c);
     *
     * c.toArray(new LongRange(0, Config.maxChunkSize)); }
     */

    public static void main(String[] args) {
        SquareRange rangeX =
                new SquareRange(new LongRange(100, 110), new LongRange(10,20));
        SquareChunk<String> chunkXstr =
                new SquareChunk<>(rangeX, (Long i1, Long i2)->{
                    return "["+i1+":"+i2+"]";
                });
        chunkXstr.forEach((String str)-> {
            System.out.print(str);
        });
        System.out.println();
        chunkXstr.forEach((Pair<Long,Long> x, String str)->{
            System.out.println("["+x.first +","+x.second+":"+str+"]");
        });
        SquareRange rangeY =
                new SquareRange(new LongRange(102, 105), new LongRange(12,15));
        chunkXstr.forEach(rangeY, (SquareSiblingAccessor<String> acc)->{
            System.out.println("SIB[" + acc.get(0, 0) + "::"
                    + acc.get(0,-1) + ":"+ acc.get(0,1)+ "^"+
                    acc.get(-1,0)+"_"+acc.get(1,0)+"]");
        });
    }
}
