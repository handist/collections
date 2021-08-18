package handist.collections;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.function.Consumer;

public class SquareRangedListView<T> implements SquareRangedList<T> {
    final SquareRangedList<T> base;

    public SquareRangedListView(SquareRangedList<T> base, SquareRange subrange) {
        this.base = base;
        this.subrange = base.getRange().intersectionCheck(subrange);
    }

    final SquareRange subrange;

    @Override
    public RangedList<T> getRowView(long row) {
        getRange().containsRowCheck(row);
        return base.getRowView(row).subList(subrange.columnRange(row));
    }

    @Override
    public RangedList<T> getColumnView(long column) {
        getRange().containsColumnCheck(column);
        return base.getColumnView(column).subList(subrange.rowRange(column));
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction) {
        base.forEachColumn(subrange.inner, (long column, RangedList<T> cView)->{
            columnAction.accept(column, cView.subList(subrange.rowRange(column)));
        });
    }

    @Override
    public void forEachColumn(LongRange columnRange, LongTBiConsumer<RangedList<T>> columnAction) {
        base.forEachColumn(subrange.inner, (long column, RangedList<T> cView)->{
            columnAction.accept(column, cView.subList(subrange.rowRange(column).intersection(columnRange)));
        });
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<T>> rowAction) {
        base.forEachRow(subrange.outer, (long row, RangedList<T> rView)->{
            rowAction.accept(row, rView.subList(subrange.columnRange(row)));
        });
    }

    @Override
    public void forEachRow(LongRange rowRange, LongTBiConsumer<RangedList<T>> rowAction) {
        base.forEachRow(subrange.outer, (long row, RangedList<T> rView)->{
            rowAction.accept(row, rView.subList(subrange.columnRange(row).intersection(rowRange)));
        });
    }

    @Override
    public RangedList<RangedList<T>> asRowList() {
        // TODO revision
        return new Chunk<RangedList<T>>(getRange().outer, (Long rowIndex)->{
            return getRowView(rowIndex);
        });
    }

    @Override
    public RangedList<RangedList<T>> asColumnList() {
        // TODO revision lazy list
        return new Chunk<RangedList<T>>(getRange().inner, (Long columnIndex)->{
            return getColumnView(columnIndex);
        });
    }

    @Override
    public void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<T>> action) {
        if(subrange.isUpperTriangle || range.isUpperTriangle) {
            throw new UnsupportedOperationException("Method forEachWithSiblings() does not support triangle ranges.");
        }
        range = getRange().intersectionCheck(range);
        base.forEachWithSiblings(range, action);
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super T> action) {
        base.forEach(subrange, action); // filter dependent
    }

    @Override
    public void forEach(SquareRange range, SquareIndexTConsumer<? super T> action) {
        range = getRange().intersectionCheck(range);
        base.forEach(range, action); // filter dependent
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        base.forEach(subrange, action); // filter dependent
    }

    @Override
    public void forEach(SquareRange range, Consumer<? super T> action) {
        // TODO rance check
        range = getRange().intersection(range);
        base.forEach(range, action); // filter dependent
    }

    @Override
    public T get(long index, long index2) {
        // TODO range check
        getRange().containsCheck(index, index2);
        return base.get(index, index2);
    }

    @Override
    public SquareRange getRange() {
        return subrange;
    }

    @Override
    public T set(long index, long index2, T value) {
        getRange().containsCheck(index, index2);
        return base.set(index, index2, value);
    }

    @Override
    public SquareRangedList<T> subView(SquareRange range) {
        range = getRange().intersection(range);
        return new SquareRangedListView<T>(base, range);
    }
}
