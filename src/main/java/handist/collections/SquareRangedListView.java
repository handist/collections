package handist.collections;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.function.Consumer;

public class SquareRangedListView<T> implements SquareRangedList<T>{
    final SquareRangedList<T> base;

    public SquareRangedListView(SquareRangedList<T> base, SquareRange subrange) {
        this.base = base;
        this.subrange = subrange;
    }

    final SquareRange subrange;

    @Override
    public RangedList<T> getRowView(long row) {
        // TODO row check
        return base.getRowView(row).subList(subrange.inner);
    }

    @Override
    public RangedList<T> getColumnView(long column) {
        // TODO column check
        return base.getColumnView(column).subList(subrange.outer);
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction) {
        base.forEachColumn((long column, RangedList<T> cView)->{
            columnAction.accept(column, cView.subList(subrange.outer));
        });
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<T>> rowAction) {
        base.forEachColumn((long row, RangedList<T> rView)->{
            rowAction.accept(row, rView.subList(subrange.inner));
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
         //TODO rangecheck
        base.forEachWithSiblings(range, action);
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super T> action) {
        base.forEach(subrange, action);
    }

    @Override
    public void forEach(SquareRange range, SquareIndexTConsumer<? super T> action) {
        // TODO range check
        base.forEach(range, action);
    }

    @Override
    public void forEach(Consumer<? super T> action) {
        base.forEach(subrange, action);
    }

    @Override
    public void forEach(SquareRange range, Consumer<? super T> action) {
        // TODO rance check
        base.forEach(range, action);
    }

    @Override
    public T get(long index, long index2) {
        // TODO range check
        rangeCheck(index, index2);
        return base.get(index, index2);
    }

    @Override
    public SquareRange getRange() {
        return subrange;
    }

    @Override
    public T set(long index, long index2, T value) {
        rangeCheck(index, index2);
        return base.set(index, index2, value);
    }

    private void rangeCheck(long outer, long inner) {
        if(!subrange.outer.contains(outer)) throw new IndexOutOfBoundsException("" + outer + " is not includeded in range:" + subrange.outer);
        if(!subrange.inner.contains(inner)) throw new IndexOutOfBoundsException("" + inner + " is not includeded in range:" + subrange.inner);
    }

    //@Override
    public SquareRangedList<T> subView(SquareRange range) {
        return new SquareRangedListView<T>(base, range);
    }
}
