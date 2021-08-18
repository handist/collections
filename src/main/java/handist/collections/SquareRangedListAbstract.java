package handist.collections;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * SquareRangedListAbstract is a general interface for SquareRangedList and RangedListProduct.
 *
 * @param <T> represents the type of each element
 * @param <X> represents the type used for the return type of method {@Code subview()} or {@Code split()} methods. In practice, SquareChunk or SquareRangedListView uses SquareRangedList as X and RangedListProduct uses RangedListProduct as X.
 */
public interface SquareRangedListAbstract<T, X extends SquareRangedListAbstract<T, X>> {
    RangedList<T> getRowView(long row);

    RangedList<T> getColumnView(long column);

    void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction);
    void forEachColumn(LongRange range, LongTBiConsumer<RangedList<T>> columnAction);
    void forEachRow(LongTBiConsumer<RangedList<T>> rowAction);
    void forEachRow(LongRange range, LongTBiConsumer<RangedList<T>> rowAction);

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

    X subView(SquareRange range);

    default List<X> split(int outer, int inner) {
        List<X> results = new ArrayList<>();
        getRange().split(outer,inner).forEach((SquareRange range)->{
            results.add(subView(range));
        });
        return results;
    }
    default List<List<X>> splitN(int outer, int inner, int num, boolean randomize) {
        List<X> flat = split(outer,inner);
        if(randomize) Collections.shuffle(flat);
        List<List<X>> results = new ArrayList<>();
        int div = flat.size() / num;
        int rem = flat.size() % num;
        int current = 0;
        for(int i=0; i<num; i++) {
            int next = current + div + (i<rem? 1:0);
            results.add(flat.subList(current, next));
            current = next;
        }
        return results;
    }

}
