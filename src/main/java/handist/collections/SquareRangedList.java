package handist.collections;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.function.Consumer;

public interface SquareRangedList<T> {
    RangedList<T> getRowView(long row);

    RangedList<T> getColumnView(long column);

    void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction);

    void forEachRow(LongTBiConsumer<RangedList<T>> rowAction);

    RangedList<RangedList<T>> asRowList();

    RangedList<RangedList<T>> asColumnList();

    void forEach(SquareRange range, Consumer<SquareSiblingAccessor<T>> action);

    void forEach(SquareIndexTConsumer<? super T> action);

    void forEach(Consumer<? super T> action);

    T get(long index, long index2);

    SquareRange getRange();

    T set(long index, long index2, T value);
}
