package handist.collections;

import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.util.ListUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
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
    default List<X> getViews(List<SquareRange> ranges) {
        List<X> results = new ArrayList<>();
        ranges.forEach((SquareRange range)->{
            results.add(subView(range));
        });
        return results;
    }

    T set(long index, long index2, T value);
    X subView(SquareRange range);

    default List<X> split(int outer, int inner) {
        return getViews(splitRange(outer,inner));
    }
    default List<List<X>> splitN(int outer, int inner, int num, boolean randomize) {
        List<SquareRange> ranges = splitRange(outer,inner);
        if(randomize) Collections.shuffle(ranges);
        List<List<X>> results = new ArrayList<>();
        for(int i=0; i<num; i++) {
            List<SquareRange> assigned = ListUtil.splitGet(ranges, i, num);
            results.add(getViews(assigned));
        }
        return results;
    }
    default List<List<X>> splitNM(int outer, int inner, int ithHost, int numHosts, int numThreads, Random rand) {
        List<SquareRange> ranges = splitRange(outer,inner);
        if(rand!=null) Collections.shuffle(ranges, rand);
        List<List<X>> results = new ArrayList<>();
        for(int i=0; i<numThreads; i++) {
            List<SquareRange> assigned = ListUtil.splitGet2(ranges, ithHost, numHosts, i, numThreads);
            results.add(getViews(assigned));
        }
        return results;
    }

    default List<SquareRange> splitRange(int outer, int inner) {
        return getRange().split(outer, inner);
    }

}
