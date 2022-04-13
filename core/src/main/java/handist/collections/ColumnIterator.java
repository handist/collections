package handist.collections;

import java.util.Iterator;

public class ColumnIterator<T> implements Iterator<T> {
    private final Object[] a;
    public final LongRange range;
    private final int stride;
    private int i; // offset inside the chunk
    private final int limit;

    public ColumnIterator() {
        this.range = new LongRange(0);
        this.a = null;
        this.limit = 0;
        this.stride = 1;
        this.i = -1;
    }

    public ColumnIterator(int offset, LongRange range, Object[] a, int stride) {
        /*
         * range0 = chunk.getRange().intersection(range0); if(range0 == null) { throw
         * new IndexOutOfBoundsException(); }
         */
        this.range = range;
        this.a = a;
        this.limit = offset + (int) range.size() * stride;
        this.stride = stride;
        this.i = offset - stride;
    }

    public ColumnIterator(LongRange range, Object[] a, int stride) {
        this.range = range;
        this.a = a;
        this.limit = (int) range.size() * stride;
        this.stride = stride;
        this.i = -1;
    }

    @Override
    public boolean hasNext() {
        return i + stride < limit;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        if (!hasNext()) {
            throw new IndexOutOfBoundsException();
        }
        i += stride;
        return (T) a[i];
    }
}
