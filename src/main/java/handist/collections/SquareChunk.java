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
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.function.SquareIndexTFunction;

/**
 * @author yoshikikawanishi
 *
 * @param <T> type handled by the collection
 */
public class SquareChunk<T> implements Serializable, SquareRangedList<T>, KryoSerializable {

    /**
     * The class to create view for the specific column range in Square.
     *
     * @param <T> type handled by the collection
     */
    static class ColumnView<T> extends MyView<T> {
        private final int offset;
        private final LongRange baseRange;
        private final int stride;
        private final Object[] a;
        // TODO sublist based impl of iterators.

        public ColumnView(int offset, int stride, LongRange baseRange, Object[] a) {
            this.offset = offset;
            this.baseRange = baseRange;
            this.stride = stride;
            this.a = a;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <U> void forEachImpl(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
                Consumer<? super U> receiver) {
            rangeCheck(range);
            long current = range.from;
            int index = offset + (int) (range.from - baseRange.from);
            while (current++ < range.to) {
                action.accept((T) a[index], receiver);
                index += stride;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEachImpl(LongRange range, Consumer<? super T> action) {
            rangeCheck(range);
            long current = range.from;
            int index = offset + (int) (range.from - baseRange.from);
            while (current++ < range.to) {
                action.accept((T) a[index]);
                index += stride;
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEachImpl(LongRange range, LongTBiConsumer<? super T> action) {
            rangeCheck(range);
            long current = range.from;
            int index = offset + (int) (range.from - baseRange.from) * stride;
            while (current < range.to) {
                action.accept(current++, (T) a[index]);
                index += stride;
            }
        }

        @Override
        public LongRange getRange() {
            return baseRange;
        }

        @Override
        public Iterator<T> iterator() {
            return new ColumnIterator<>(offset, baseRange, a, stride);
        }

        @Override
        public RangedListIterator<T> listIterator() {
            return new ColumnListIterator<>(offset, baseRange, a, stride);
        }

        @Override
        public RangedListIterator<T> listIterator(long from) {
            return new ColumnListIterator<>(offset, baseRange, from, a, stride);
        }

        @Override
        public void rangeCheck(LongRange range) {
            if (!baseRange.contains(range)) {
                throw new IndexOutOfBoundsException(
                        "[ColumnView] range mismatch: " + this.getRange() + " does not include range");
            }
        }

        @Override
        protected Iterator<T> subIterator(LongRange range) {
            // TODO check
            final int newOffset = offset + (int) (range.from - baseRange.from) * stride;
            return new ColumnIterator<>(newOffset, range, a, stride);
        }

        @Override
        protected RangedListIterator<T> subListIterator(LongRange range) {
            final int newOffset = offset + (int) (range.from - baseRange.from) * stride;
            return new ColumnListIterator<>(newOffset, range, a, stride);
        }

        @Override
        protected RangedListIterator<T> subListIterator(LongRange range, long from) {
            final int newOffset = offset + (int) (range.from - baseRange.from) * stride;
            return new ColumnListIterator<>(newOffset, range, from, a, stride);
        }
    }

    /**
     * The class to convert 1D to 2D.
     *
     * @param <T> type handled by the collection
     */
    static class MySquareSiblingAccessor<T> implements SquareSiblingAccessor<T> {
        private final int offset;
        private final int innerSize;
        private final Object[] a;

        /**
         * @param offset    the offset of index
         * @param innerSize the size of inner loop
         * @param a         the base array
         */
        public MySquareSiblingAccessor(int offset, int innerSize, Object[] a) {
            this.offset = offset;
            this.innerSize = innerSize;
            this.a = a;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T get(int x, int y) {
            assert (x <= 1 && x >= -1);
            assert (y <= 1 && y >= -1);
            return (T) a[offset + x * innerSize + y];
        }

        @Override
        public void put(T v) {
            a[offset] = v;
        }
    }

    /**
     * The class for implementing view of row and column.
     *
     * @param <T> type handled by the collection
     */
    static abstract class MyView<T> extends RangedList<T> {
        @Override
        public RangedList<T> cloneRange(LongRange range) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public boolean contains(Object o) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public T get(long index) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public abstract void rangeCheck(LongRange range);

        @Override
        public T set(long index, T value) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public Object[] toArray(LongRange r) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public Chunk<T> toChunk(LongRange r) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }

        @Override
        public List<T> toList(LongRange r) {
            throw new UnsupportedOperationException("not implemented yet. may be shared abst class will be needed");
        }
    }

    /**
     * The class to create view for the specific row range in Square.
     */
    static class RowView<T> extends MyView<T> {
        private final int offset;
        private final LongRange baseRange;
        private final Object[] a;
        // TODO sublist based impl of iterators.

        public RowView(int offset, LongRange baseRange, Object[] a) {
            this.offset = offset;
            this.baseRange = baseRange;
            this.a = a;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <U> void forEachImpl(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
                Consumer<? super U> receiver) {
            rangeCheck(range);
            long current = range.from;
            int index = offset + (int) (range.from - baseRange.from);
            while (current++ < range.to) {
                action.accept((T) a[index++], receiver);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEachImpl(LongRange range, Consumer<? super T> action) {
            rangeCheck(range);
            long current = range.from;
            int index = offset + (int) (range.from - baseRange.from);
            while (current++ < range.to) {
                action.accept((T) a[index++]);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEachImpl(LongRange range, LongTBiConsumer<? super T> action) {
            rangeCheck(range);
            long current = range.from;
            int index = offset + (int) (range.from - baseRange.from);
            while (current < range.to) {
                action.accept(current++, (T) a[index++]);
            }
        }

        @Override
        public LongRange getRange() {
            return baseRange;
        }

        @Override
        public Iterator<T> iterator() {
            return new ChunkIterator<>(offset, baseRange, a);
        }

        @Override
        public RangedListIterator<T> listIterator() {
            return new ChunkListIterator<>(offset, baseRange, a);
        }

        @Override
        public RangedListIterator<T> listIterator(long from) {
            return new ChunkListIterator<>(offset, baseRange, from, a);
        }

        @Override
        public void rangeCheck(LongRange range) {
            if (!baseRange.contains(range)) {
                throw new IndexOutOfBoundsException(
                        "[RowView] range mismatch: " + this.getRange() + " does not include range");
            }
        }

        @Override
        protected Iterator<T> subIterator(LongRange range) {
            rangeCheck(range);
            final int newOffset = offset + (int) (range.from - baseRange.from);
            return new ChunkIterator<>(newOffset, range, a);
        }

        @Override
        protected RangedListIterator<T> subListIterator(LongRange range) {
            rangeCheck(range);
            final int newOffset = offset + (int) (range.from - baseRange.from);
            return new ChunkListIterator<>(newOffset, range, a);
        }

        @Override
        protected RangedListIterator<T> subListIterator(LongRange range, long from) {
            rangeCheck(range);
            final int newOffset = offset + (int) (range.from - baseRange.from);
            return new ChunkListIterator<>(newOffset, range, from, a);
        }
    }

    private static final long serialVersionUID = -8282656629771334491L;

    /** Array containing the T objects */
    private Object[] a;
    /** Range on which this instance is defined */
    private SquareRange range;
    /**  */
    private long innerSize;

    /**
     * Builds a SquareChunk with the given range and no mapping.
     * <p>
     * The given SquareRange should have a strictly positive size. Giving a
     * {@link SquareRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link SquareRange} provided has a range that exceeds
     * {@link Config#maxChunkSize} , an {@link IllegalArgumentException} will be be
     * thrown.
     *
     * @param range the range of the chunk to build
     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built
     *                                  with the provided range.
     */
    public SquareChunk(SquareRange range) {
        final long outerSize = range.outer.size();
        this.innerSize = range.inner.size();
        final long size = outerSize * innerSize;
        if (range.isUpperTriangle) {
            throw new IllegalArgumentException("Rectangle ranges are not supported by SquareChunk yet.");
        }
        // FIXME
        if (size > Integer.MAX_VALUE) {
            throw new RuntimeException("array size overflow");
        }
        if (size > Config.maxChunkSize) {
            throw new IllegalArgumentException(
                    "The given range " + range + " exceeds the maximum Chunk size " + Config.maxChunkSize);
        }
        a = new Object[(int) size];
        this.range = range;
    }

    /**
     * Builds a {@link SquareChunk} with the provided {@link SquareRange} and an
     * initial mapping for each long in the object array. The provided
     * {@link SquareRange} and Object array should have the same size. An
     * {@link IllegalArgumentException} will be thrown otherwise.
     * <p>
     * The given {@link SquareRange} should have a strictly positive size. Giving a
     * {@link SquareRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link SquareRange} provided has a range that exceeds
     * {@link Config#maxChunkSize}, an {@link IllegalArgumentException} will be be
     * thrown.
     *
     * @param range the range of the chunk to build
     * @param a     array with the initial mapping for every long in the provided
     *              range
     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built
     *                                  with the provided range and object array.
     */
    public SquareChunk(SquareRange range, Object[] a) {
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
     * Builds a {@link SquareChunk} with the provided {@link SquareRange}. The
     * provided initializer generates the initial value of the element for each
     * index. The given SquareRange should have a strictly positive size. Giving a
     * {@link SquareRange} instance with identical lower and upper bounds will
     * result in a {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link SquareRange} provided has a range that exceeds
     * {@link Config#maxChunkSize}, an {@link IllegalArgumentException} will be be
     * thrown.
     *
     * @param range       the range of the Squarechunk to build
     * @param initializer generates the initial value of the element for each index.
     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built
     *                                  with the provided range.
     */
    public SquareChunk(SquareRange range, SquareIndexTFunction<T> initializer) {
        this(range);
        range.forEach((long index, long index2) -> {
            set(index, index2, initializer.apply(index, index2));
        });
    }

    /**
     * Builds a {@link SquareChunk} with the provided {@link SquareRange} with each
     * long in the provided range mapped to object t. The given LongRange should
     * have a strictly positive size. Giving a {@link SquareRange} instance with
     * identical lower and upper bounds will result in a
     * {@link IllegalArgumentException} being thrown.
     * <p>
     * If the {@link SquareRange} provided has a range that exceeds
     * {@link Config#maxChunkSize}, an {@link IllegalArgumentException} will be be
     * thrown.
     *
     * @param range the range of the chunk to build
     * @param t     initial mapping for every long in the provided range
     * @throws IllegalArgumentException if a {@link SquareChunk} cannot be built
     *                                  with the provided range.
     */
    public SquareChunk(SquareRange range, T t) {
        this(range);
        // TODO Is this what we really want to do?
        // The mapping will be on the SAME OBJECT for every long in LongRange.
        // Don't we need a Generator<T> generator as argument and create an
        // instance for each key with Arrays.setAll(a, generator) ?
        Arrays.fill(a, t);
    }

    @Override
    public RangedList<RangedList<T>> asColumnList() {
        return new Chunk<>(range.inner, (Long columnIndex) -> {
            return getColumnView(columnIndex);
        });
    }

    @Override
    public RangedList<RangedList<T>> asRowList() {
        return new Chunk<>(range.outer, (Long rowIndex) -> {
            return getRowView(rowIndex); // OK filter dependet
        });
    }

//  /**
//  * Returns a new Chunk defined on the same {@link LongRange} and with the same
//  * contents as this instance.
//  *
//  * @return a copy of this instance
//  */
//     @Override
//     public SquareChunk<T> clone() {
//         // Object[] aClone = a.clone();
//         final Object[] aClone = new Object[a.length];
//
//         //// FIXME: 2018/09/19 Need deep copy?
//         // for (int i = 0; i < a.length; i++) {
//         // try {
//         // aClone[i] = ((Cloneable) a[i]).clone();
//         // } catch (CloneNotSupportedException e) {
//         // e.printStackTrace();
//         // }
//         // }
//
//         Arrays.fill(aClone, a[0]);
//         System.arraycopy(a, 0, aClone, 0, a.length);
//
//         return new SquareChunk<>(this.range, aClone);
//     }

//    @Override
//    public SquareChunk<T> cloneRange(LongRange newRange) {
//        return range == newRange ? clone() : toChunk(newRange);
//    }
//
    @Override
    public boolean contains(Object v) {
        for (final Object e : a) {
            if (v == null ? e == null : v.equals(e)) {
                return true;
            }
        }
        return false;
    }

    /**
     * (<em>&lt;tag&gt;</em>), row:&lt;rowIndex&gt; ->
     * [&lt;columnIndex&gt:&lt;element&gt;][&lt;columnIndex&gt:&lt;element&gt;]...<br>
     * (<em>&lt;tag&gt;</em>), row:&lt;rowIndex&gt; ->
     * [&lt;columnIndex&gt:&lt;element&gt;][&lt;columnIndex&gt:&lt;element&gt;]...<br>
     * ...
     *
     * @param tag header tag
     */
    void debugPrint(String tag) {
        debugPrint(tag, new PrintWriter(System.out));
    }

    /**
     * (<em>&lt;tag&gt;</em>), row:&lt;rowIndex&gt; ->
     * [&lt;columnIndex&gt:&lt;element&gt;][&lt;columnIndex&gt:&lt;element&gt;]...<br>
     * (<em>&lt;tag&gt;</em>), row:&lt;rowIndex&gt; ->
     * [&lt;columnIndex&gt:&lt;element&gt;][&lt;columnIndex&gt:&lt;element&gt;]...<br>
     * ...
     *
     * @param tag header tag
     * @param out {@link PrintWriter} to output
     */
    void debugPrint(String tag, PrintWriter out) {
        forEachRow((long index, RangedList<T> row) -> {
            out.print("(" + tag + "), row:" + index + "->");
            row.forEach((long i, T e) -> {
                out.print("[" + i + ":" + e + "]");
            });
            out.println();
        });
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof SquareRangedListAbstract)) {
            return false;
        }
        final SquareRangedList<?> target = (SquareRangedList<?>) o;
        // TODO this version is too slow,
        // setupFrom will be the good candidate for fast simul scanner.
        if (!getRange().equals(target.getRange())) {
            return false;
        }
        for (final long i : getRange().outer) {
            for (final long j : getRange().inner) {
                if (!get(i, j).equals(target.get(i, j))) {
                    return false;
                }
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void forEach(final Consumer<? super T> action) {
        rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        for (final Object o : a) {
            action.accept((T) o); // TODO filter dependent
        }
    }

    @Override
    public void forEach(final SquareIndexTConsumer<? super T> action) {
        rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        long index = range.outer.from;
        long index2 = range.inner.from;
        int offset = 0;
        while (true) {
            @SuppressWarnings("unchecked")
            final T v = (T) a[offset];
            action.accept(index, index2, v);
            index2++;
            offset++;
            if (index2 == range.inner.to) {
                index2 = range.inner.from;
                index++;
                if (index == range.outer.to) {
                    return;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void forEach(SquareRange range, final Consumer<? super T> action) {
        rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        long index1 = range.outer.from;
        long index2 = range.inner.from;
        long offset1 = index1 - getRange().outer.from;
        final long offset2 = index2 - getRange().inner.from;
        // TODO overflow assert
        int offset = (int) (offset1 * innerSize + offset2);
        while (true) {
            // TODO overflow assert
            action.accept((T) a[offset]);
            index2++;
            offset++;
            if (index2 == range.inner.to) {
                index2 = range.inner.from; // TODO filter dependent
                index1++;
                if (index1 == range.outer.to) {
                    return;
                }
                offset1++;
                offset = (int) (offset1 * innerSize + offset2);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void forEach(SquareRange range, final SquareIndexTConsumer<? super T> action) {
        rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        long index1 = range.outer.from;
        long index2 = range.inner.from;
        long offset1 = index1 - getRange().outer.from;
        final long offset2 = index2 - getRange().inner.from;
        // TODO overflow assert
        int offset = (int) (offset1 * innerSize + offset2);
        while (true) {
            // TODO overflow assert
            action.accept(index1, index2, (T) a[offset]);
            index2++;
            offset++;
            if (index2 == range.inner.to) { // TODO filter dependent
                index2 = range.inner.from;
                index1++;
                if (index1 == range.outer.to) {
                    return;
                }
                offset1++;
                offset = (int) (offset1 * innerSize + offset2);
            }
        }
    }

    @Override
    public void forEachColumn(LongRange range, LongTBiConsumer<RangedList<T>> columnAction) {
        // TODO filter support
        range = getRange().inner.intersection(range);
        for (long index = range.from; index < range.to; index++) {
            final RangedList<T> cView = getColumnView(index);
            columnAction.accept(index, cView);
        }
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction) {
        for (long index = getRange().inner.from; index < getRange().inner.to; index++) {
            final RangedList<T> cView = getColumnView(index);
            columnAction.accept(index, cView);
        }
    }

    @Override
    public void forEachRow(LongRange rRange, LongTBiConsumer<RangedList<T>> rowAction) {
        rRange = getRange().outer.intersection(rRange);
        for (long index = rRange.from; index < rRange.to; index++) {
            final RangedList<T> rView = getRowView(index);
            rowAction.accept(index, rView);
        }
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<T>> rowAction) {
        for (long index = getRange().outer.from; index < getRange().outer.to; index++) {
            final RangedList<T> rView = getRowView(index);
            rowAction.accept(index, rView);
        }
    }

    @Override
    public void forEachWithSiblings(SquareRange range, final Consumer<SquareSiblingAccessor<T>> action) {
        rangeCheck(range);
        // TODO
        // IntStream.range(begin, end).forEach();
        long index1 = range.outer.from;
        long index2 = range.inner.from;
        long offset1 = index1 - getRange().outer.from;
        final long offset2 = index2 - getRange().inner.from;
        // TODO overflow assert
        int offset = (int) (offset1 * innerSize + offset2);
        while (true) {
            // TODO overflow assert
            // System.out.println("index:" + index1 + "," + index2 + ", offset" + offset1 +
            // "," + offset2 + "," + offset);
            final SquareSiblingAccessor<T> acc = new MySquareSiblingAccessor<>(offset, (int) innerSize, a);
            action.accept(acc);
            index2++;
            offset++;
            if (index2 == range.inner.to) { // TODO filter dependent
                index2 = range.inner.from;
                index1++;
                if (index1 == range.outer.to) {
                    return;
                }
                offset1++;
                // TODO overflow assert
                offset = (int) (offset1 * innerSize + offset2);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T get(long index, long index2) { // TODO filter dependent
        if (!getRange().outer.contains(index)) {
            throw new IndexOutOfBoundsException(/* rangeMsg(index) */ index + " is outof " + getRange().outer);
        }
        if (!getRange().inner.contains(index2)) {
            throw new IndexOutOfBoundsException(/* rangeMsg(index2) */ index2 + " is outof " + getRange().inner);
        }
        return getUnsafe(index, index2);
    }

    @Override
    public RangedList<T> getColumnView(long column) {
        if (!getRange().inner.contains(column)) {
            throw new IndexOutOfBoundsException("column " + column + " is outof " + getRange());
        }
        final int offset = (int) (column - getRange().inner.from);
        return new ColumnView<>(offset, (int) innerSize, getRange().outer, a); // TODO filter dependent
    }

    @Override
    public SquareRange getRange() {
        return range;
    }

    @Override
    public RangedList<T> getRowView(long row) {
        if (!getRange().outer.contains(row)) {
            throw new IndexOutOfBoundsException("row " + row + " is outof " + getRange());
        }
        final int offset = (int) (innerSize * (row - getRange().outer.from));
        return new RowView<>(offset, getRange().inner, a); // TODO filter dependent
    }

    /**
     * Returns the element located at the provided index. The provided index is
     * presumed valid and as such, no bound checking is done.
     *
     * @param index  outer index whose value should be returned
     * @param index2 inner index whose value should be returned
     * @return the object stored at the provided index, possibly {@code null}
     */
    @SuppressWarnings("unchecked")
    final T getUnsafe(long index, long index2) { // when range check was done
        final long offset1 = index - range.outer.from;
        final long offset2 = index2 - range.inner.from;
        return (T) a[(int) (offset1 * innerSize + offset2)];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return SquareRangedListAbstract.hashCode(this);
    }

    @Override
    public Iterator<T> iterator() {
        return new SquareChunkIterator<>(range, a);
    }

    @Deprecated
    @Override
    public void read(Kryo kryo, Input input) {
        this.innerSize = input.readLong();
        this.range = (SquareRange) kryo.readClassAndObject(input);
        this.a = (Object[]) kryo.readClassAndObject(input);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.innerSize = in.readLong();
        this.range = (SquareRange) in.readObject();
        this.a = (Object[]) in.readObject();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T set(long index, long index2, T value) {
        if (!getRange().outer.contains(index)) {
            throw new IndexOutOfBoundsException(/* rangeMsg(index) */ index + " is outof " + getRange().outer);
        }
        if (!getRange().inner.contains(index2)) {
            throw new IndexOutOfBoundsException(/* rangeMsg(index2) */ index2 + " is outof " + getRange().inner);
        }
        return setUnsafe(index, index2, value);
    }

    @SuppressWarnings("unchecked")
    private T setUnsafe(long index, long index2, T v) { // when range check was done
        final long offset1 = index - range.outer.from;
        final long offset2 = index2 - range.inner.from;
        final long offset = offset1 * innerSize + offset2;
        final T prev = (T) a[(int) offset];
        a[(int) offset] = v;
        return prev;
    }

    /**
     * Initializes the values in this instance by applying the provided function on
     * the elements contained in {@code source}
     *
     * @param <S>    the type handled by the {@link RangedList} given as parameter,
     *               input for the function
     * @param source {@link SquareChunk} instance from which entried for this
     *               instance will be extracted
     * @param func   function that takes an object of type S as parameter and
     *               returns a type T
     */
    public <S> void setupFrom(SquareChunk<S> source, Function<? super S, ? extends T> func) {
        // TODO 'source' should be SquareRangedList
        // and this method also should be declared in SquareRangedList
        rangeCheck(source.getRange());
        // TODO filter dependent
        final SquareIndexTConsumer<S> consumer = (long index1, long index2, S s) -> {
            final T r = func.apply(s);
            final long offset1 = index1 - getRange().outer.from;
            final long offset2 = index2 - getRange().inner.from;
            final long offset = offset1 * innerSize + offset2;
            a[(int) offset] = r;
        };
        source.forEach(consumer);
    }

    @Override
    public Iterator<T> subIterator(SquareRange range) {
        final SquareRange r = range.intersection(getRange());
        return new SquareChunkIterator<>(r, this.range, a);
    }

    @Override
    public SquareRangedList<T> subView(SquareRange range) {
        return new SquareRangedListView<>(this, range);
    }

    @Override
    public Object[] toArray() {
        return a;
    }

    @Override
    public Object[] toArray(SquareRange newRange) {
        // TODO : now copy array. more efficient implementation ?
        final Object[] newRail = new Object[(int) newRange.size()];
        final int[] count = { 0 };
        forEach(newRange, (T t) -> {
            newRail[count[0]] = t;
            count[0]++;
        });
        return newRail;
    }

    @Override
    public SquareChunk<T> toChunk(SquareRange newRange) {
        // TODO : now copy chunk. more efficient implementation ?
        final SquareChunk<T> newChunk = new SquareChunk<>(newRange);
        forEach(newRange, (i, j, t) -> {
            newChunk.set(i, j, t);
        });
        return newChunk;
    }

    @Override
    public List<T> toList(SquareRange newRange) {
        final ArrayList<T> list = new ArrayList<>((int) newRange.size());
        forEach(newRange, (t) -> list.add(t)); // check range at forEach
        return list;
    }

    @Override
    public String toString() {
        final StringWriter out = new StringWriter();
        debugPrint("  ", new PrintWriter(out));
        return out.toString();
    }

    @Deprecated
    @Override
    public void write(Kryo kryo, Output output) {
        output.writeLong(innerSize);
        kryo.writeClassAndObject(output, range);
        kryo.writeClassAndObject(output, a);
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeLong(innerSize);
        out.writeObject(range);
        out.writeObject(a);
    }

}
