package handist.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Consumer;

public interface RangedList<T> extends Collection<T> {

    LongRange getRange();

    T get(long index);
    T set(long index, T value);

    T first();
    T last();

    long longSize();

    void each(Consumer<T> action);
    void each(LongRange range, Consumer<T> action);

    RangedList<T> cloneRange(LongRange newRange);
    Chunk<T> toChunk(LongRange newRange);
    Object[] toArray();
    Object[] toArray(LongRange newRange);

    RangedList<T> subList(long begin, long end);
    Iterator<T> iteratorFrom(long i);
}
