package handist.collections;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

public class SquareRangedListView<T> implements SquareRangedList<T> {
    private final SquareRangedList<T> base;

    private final SquareRange subRange;

    /**
     * Creates a new {@link SquareRangedListView} which grants access to the
     * provided {@link SquareRangedList} only on the specified range.
     * <p>
     * The provided base can either be a {@link SquareChunk} or
     * {@link RangedListProduct} or an existing {@link SquareRangedListView}, in
     * which case the {@link SquareChunk} base of this {@link RangedListView} will
     * be extracted.
     *
     * @param base     {@link SquareRangedList} this instance will control access to
     * @param subRange the range of indices that the created instance allows access
     *                 to
     */
    public SquareRangedListView(SquareRangedList<T> base, SquareRange subRange) {
        this.base = base;
        this.subRange = base.getRange().intersectionCheck(subRange);
    }

    @Override
    public RangedList<RangedList<T>> asColumnList() {
        // TODO revision lazy list
        return new Chunk<>(getRange().inner, (Long columnIndex) -> {
            return getColumnView(columnIndex);
        });
    }

    @Override
    public RangedList<RangedList<T>> asRowList() {
        // TODO revision
        return new Chunk<>(getRange().outer, (Long rowIndex) -> {
            return getRowView(rowIndex);
        });
    }

    @Override
    public boolean contains(Object v) {
        for (long i = subRange.outer.from; i < subRange.outer.to; i++) {
            for (long j = subRange.inner.from; j < subRange.inner.to; j++) {
                if (base.get(i, j).equals(v)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        base.forEach(subRange, action); // filter dependent
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super T> action) {
        base.forEach(subRange, action); // filter dependent
    }

    @Override
    public void forEach(SquareRange range, Consumer<? super T> action) {
        range = getRange().intersection(range);
        base.forEach(range, action); // filter dependent
    }

    @Override
    public void forEach(SquareRange range, SquareIndexTConsumer<? super T> action) {
        range = getRange().intersectionCheck(range);
        base.forEach(range, action); // filter dependent
    }

    @Override
    public void forEachColumn(LongRange columnRange, LongTBiConsumer<RangedList<T>> columnAction) {
        base.forEachColumn(subRange.inner, (long column, RangedList<T> cView) -> {
            columnAction.accept(column, cView.subList(subRange.rowRange(column).intersection(columnRange)));
        });
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction) {
        base.forEachColumn(subRange.inner, (long column, RangedList<T> cView) -> {
            columnAction.accept(column, cView.subList(subRange.rowRange(column)));
        });
    }

    @Override
    public void forEachRow(LongRange rowRange, LongTBiConsumer<RangedList<T>> rowAction) {
        base.forEachRow(subRange.outer, (long row, RangedList<T> rView) -> {
            rowAction.accept(row, rView.subList(subRange.columnRange(row).intersection(rowRange)));
        });
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<T>> rowAction) {
        base.forEachRow(subRange.outer, (long row, RangedList<T> rView) -> {
            rowAction.accept(row, rView.subList(subRange.columnRange(row)));
        });
    }

    @Override
    public void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<T>> action) {
        if (subRange.isUpperTriangle || range.isUpperTriangle) {
            throw new UnsupportedOperationException("Method forEachWithSiblings() does not support triangle ranges.");
        }
        range = getRange().intersectionCheck(range);
        base.forEachWithSiblings(range, action);
    }

    @Override
    public T get(long index, long index2) {
        rangeCheck(index, index2);
        return base.get(index, index2);
    }

    @Override
    public RangedList<T> getColumnView(long column) {
        getRange().containsColumnCheck(column);
        return base.getColumnView(column).subList(subRange.rowRange(column));
    }

    @Override
    public SquareRange getRange() {
        return subRange;
    }

    @Override
    public RangedList<T> getRowView(long row) {
        getRange().containsRowCheck(row);
        return base.getRowView(row).subList(subRange.columnRange(row));
    }

    @Override
    public Iterator<T> iterator() {
        return base.subIterator(subRange);
    }

    @Override
    public T set(long index, long index2, T value) {
        getRange().containsCheck(index, index2);
        return base.set(index, index2, value);
    }

    @Override
    public Iterator<T> subIterator(SquareRange range) {
        final SquareRange r = range.intersection(getRange());
        return new SquareChunkIterator<>(r, getRange(), base.toArray());
    }

    @Override
    public SquareRangedList<T> subView(SquareRange range) {
        range = getRange().intersection(range);
        return new SquareRangedListView<>(base, range);
    }

    @Override
    public Object[] toArray() {
        return base.toArray(subRange);
    }

    @Override
    public Object[] toArray(SquareRange newRange) {
        rangeCheck(newRange);
        return base.toArray(newRange);
    }

    @Override
    public SquareChunk<T> toChunk(SquareRange newRange) {
        rangeCheck(newRange);
        return base.toChunk(newRange);
    }

    @Override
    public List<T> toList(SquareRange newRange) {
        rangeCheck(newRange);
        return base.toList(newRange);
    }
}
