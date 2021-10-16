package handist.collections;

import handist.collections.function.LongTBiFunction;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class LazyRangedList<S,T> extends RangedList<T> {
    private final RangedList<S> base;
    private final LongTBiFunction<S,T> func;

    static class Iter<S,T> implements Iterator<T> {
        private final Iterator<S> base;
        private final LongTBiFunction<S,T> func;
        private long index;

        Iter(Iterator<S> base, LongTBiFunction<S,T> func, long index) {
            this.base = base;
            this.func = func;
            this.index = index;
        }

        @Override
        public boolean hasNext() {
            return base.hasNext();
        }

        @Override
        public T next() {
            return func.apply(index++, base.next());
        }
    }

    LazyRangedList(RangedList<S> base, LongTBiFunction<S,T> func) {
        this.base = base;
        this.func = func;
    }

    @Override
    public RangedList<T> cloneRange(LongRange range) {
        return new LazyRangedList<S,T>(base.cloneRange(range), func);
    }

    @Override
    public boolean contains(Object o) {
        throw new UnsupportedOperationException("not supported.");
    }

    @Override
    public T get(long index) {
        return func.apply(index, base.get(index));
    }

    @Override
    public LongRange getRange() {
        return base.getRange();
    }

    @Override
    public Iterator<T> iterator() {
        return new Iter<S,T>(base.iterator(), func, base.getRange().from);
    }

    @Override
    public RangedListIterator<T> listIterator() {
        throw new UnsupportedOperationException("not supported.");
    }

    @Override
    public RangedListIterator<T> listIterator(long from) {
        throw new UnsupportedOperationException("not supported.");
    }

    @Override
    protected Iterator<T> subIterator(LongRange range) {
        return new Iter<S,T>(base.subIterator(range), func, range.from);
    }

    @Override
    protected RangedListIterator<T> subListIterator(LongRange range) {
        throw new UnsupportedOperationException("not supported.");
    }

    @Override
    protected RangedListIterator<T> subListIterator(LongRange range, long from) {
        throw new UnsupportedOperationException("not supported.");
    }

    @Override
    public T set(long index, T value) {
        throw new UnsupportedOperationException("set is not supported by LazyRangedList.");
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("not supported yet.");
    }

    @Override
    public Object[] toArray(LongRange r) {
        throw new UnsupportedOperationException("not supported yet.");
    }

    @Override
    public Chunk<T> toChunk(LongRange r) {
        throw new UnsupportedOperationException("not supported yet.");
    }

    @Override
    public List<T> toList(LongRange r) {
        throw new UnsupportedOperationException("not supported yet.");
    }
}
