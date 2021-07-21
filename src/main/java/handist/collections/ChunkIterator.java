package handist.collections;

import java.util.Iterator;

public class ChunkIterator<T> implements Iterator<T> {
    private final Object[] a;
    private final LongRange range;
    private int i; // offset inside the chunk
    private int limit;


    public ChunkIterator() {
        this.range = new LongRange(0);
        this.a = null;
        this.limit = 0;
        this.i = -1;
    }
    public ChunkIterator(LongRange range, Object[] a) {
        this.range = range;
        this.a = a;
        this.limit = (int)range.size();
        this.i = -1;
    }
    public ChunkIterator(int offset, LongRange range, Object[] a) {
        /*
        range0 = chunk.getRange().intersection(range0);
        if(range0 == null) {
            throw new IndexOutOfBoundsException();
        }*/
        this.range = range;
        this.a = a;
        this.limit = offset + (int)range.size();
        this.i = offset - 1;
    }

    @Override
    public boolean hasNext() {
        return i + 1 < limit;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        if(!hasNext()) throw new IndexOutOfBoundsException();
        return (T) a[++i];
    }
}
