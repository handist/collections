package handist.collections;

public class ChunkListIterator<T> implements RangedListIterator<T> {
    private final LongRange range;
    private final Object[] a;
    private int i; // offset inside the chunk
    private final int head;
    private final int limit;
    private int lastReturnedShift = -1;

    public ChunkListIterator() {
        this.a = null;
        this.range = new LongRange(0);
        this.head = 0;
        this.limit = 0;
        this.i = -1;
    }

    /**
     *
     * @param offset means the position from which the first element of range resides in the array a.
     * @param range represents the index ranges where the list iterator can go forward/back.
     * @param i0 represents the position where the iterator start scanning.
     * @param a represents the array container
     */
    public ChunkListIterator(int offset, LongRange range, long i0, Object[] a) {
        if (!range.contains(i0)) {
            throw new IndexOutOfBoundsException();
        }
        this.a = a;
        this.range = range;
        this.head = offset;
        this.limit = offset + (int)range.size();
        this.i = offset + (int)(i0-range.from) - 1;
    }
    public ChunkListIterator(int offset, LongRange range, Object[] a) {
        this.a = a;
        this.range = range;
        this.head = offset;
        this.limit = offset + (int)range.size();
        this.i = offset - 1;
    }

    public ChunkListIterator(LongRange range, Object[] a) {
        this(0, range, a);
    }
    public ChunkListIterator(LongRange range, long i0, Object[] a) {
        this(0, range, i0, a);
    }

    private LongRange iterRange() {
        return range;
    }

    @Override
    public boolean hasNext() {
        return i + 1 < limit;
    }

    @Override
    public boolean hasPrevious() {
        return i > head;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        if(!hasNext())
            throw new IndexOutOfBoundsException("[Chunk.It] range mismatch: " + iterRange() + " does not include " + i);
        lastReturnedShift = 0;
        return (T) a[++i];
    }

    @Override
    public long nextIndex() {
        return range.from + i + 1;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T previous() {
        if(!hasPrevious())
            throw new IndexOutOfBoundsException("[Chunk.It] range mismatch: " + iterRange() + " does not include " + i);
        lastReturnedShift = 1;
        return (T) a[i--];
    }

    @Override
    public long previousIndex() {
        return range.from + i;
    }

    @Override
    public void set(T e) {
        if (lastReturnedShift == -1) {
            throw new IllegalStateException("[Chunk.It] Either method "
                    + "previous or next needs to be called before method set" + " can be used");
        }
        a[i + lastReturnedShift] = e; // FIXME THIS IS NOT CORRECT !!!
    }
}
