package handist.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public interface RangedList<T> extends Collection<T> {

    LongRange getRange();

    T get(long index);
    T set(long index, T value);

    T first();
    T last();

    long longSize();

    void forEach(LongRange range, Consumer<? super T> action);
    
    default public List<RangedList<T>> splitRange(long splitPoint) {
        LongRange range = getRange();
        RangedList<T> rangedList1 = new RangedListView<T>(this, new LongRange(range.begin, splitPoint));
        RangedList<T> rangedList2 = new RangedListView<T>(this, new LongRange(splitPoint, range.end));
        return Arrays.asList(rangedList1, rangedList2);
    }

    default public List<RangedList<T>> splitRange(long splitPoint1, long splitPoint2) {
        LongRange range = getRange();
        RangedList<T> rangedList1 = new RangedListView<T>(this, new LongRange(range.begin, splitPoint1));
        RangedList<T> rangedList2 = new RangedListView<T>(this, new LongRange(splitPoint1, splitPoint2));
        RangedList<T> rangedList3 = new RangedListView<T>(this, new LongRange(splitPoint2, range.end));
        return Arrays.asList(rangedList1, rangedList2, rangedList3);
    }

    RangedList<T> cloneRange(LongRange newRange);
    Chunk<T> toChunk(LongRange newRange);
    Object[] toArray();
    Object[] toArray(LongRange newRange);

    RangedList<T> subList(long begin, long end);
    Iterator<T> iteratorFrom(long i);
}
