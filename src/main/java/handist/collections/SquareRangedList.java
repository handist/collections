package handist.collections;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

public interface SquareRangedList<T> {
    RangedList<T> getRowView(long row);

    RangedList<T> getColumnView(long column);

    void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction);
    void forEachRow(LongTBiConsumer<RangedList<T>> rowAction);

    RangedList<RangedList<T>> asRowList();
    RangedList<RangedList<T>> asColumnList();


    void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<T>> action);

    void forEach(SquareIndexTConsumer<? super T> action);
    void forEach(SquareRange subrange, SquareIndexTConsumer<? super T> action);

    void forEach(Consumer<? super T> action);
    void forEach(SquareRange subrange, Consumer<? super T> action);

    T get(long index, long index2);

    SquareRange getRange();

    T set(long index, long index2, T value);

    SquareRangedList<T> subView(SquareRange range);

    default Collection<SquareRangedList<T>> split(int outer, int inner) {
        Collection<SquareRangedList<T>> results = new ArrayList<>();
        List<LongRange> splitOuters = getRange().outer.split(outer);
        List<LongRange> splitInners = getRange().inner.split(inner);
        for (LongRange out0 : splitOuters) {
            for (LongRange in0 : splitInners) {
                results.add(subView(new SquareRange(out0, in0)));
            }
        }
        return results;
    }

}
