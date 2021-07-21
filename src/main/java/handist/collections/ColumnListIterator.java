package handist.collections;

public class ColumnListIterator<T> implements RangedListIterator<T> {
    private final LongRange range;
    private final Object[] a;
    private final int offset;
    private final int stride;
    private int i; // = index - range.from
    private final int size;
    private int lastReturnedShift = -1;

    public ColumnListIterator() {
        this.a = null;
        this.range = new LongRange(0);
        this.offset = 0;
        this.size = 0;
        this.stride = 1;
        this.i = -1;
    }

    /**
     *
     * @param offset means the position from which the first element of range resides in the array a.
     * @param range represents the index ranges where the list iterator can go forward/back.
     * @param i0 represents the position where the iterator start scanning.
     * @param a represents the array container
     */
    public ColumnListIterator(int offset, LongRange range, long i0, Object[] a, int stride) {
        if (!range.contains(i0)) {
            throw new IndexOutOfBoundsException();
        }
        this.a = a;
        this.range = range;
        this.offset = offset;
        this.size = (int)range.size();
        this.i = (int)(i0-range.from) - 1;
        this.stride = stride;
    }
    public ColumnListIterator(int offset, LongRange range, Object[] a, int stride) {
        this.a = a;
        this.offset = offset;
        this.range = range;
        this.size = (int)range.size();
        this.i = - 1;
        this.stride = stride;
    }

    public ColumnListIterator(LongRange range, Object[] a, int stride) {
        this(0, range, a, stride);
    }
    public ColumnListIterator(LongRange range, long i0, Object[] a, int stride) {
        this(0, range, i0, a, stride);
    }

    private LongRange iterRange() {
        return range;
    }

    @Override
    public boolean hasNext() {
        return i + 1 < size;
    }

    @Override
    public boolean hasPrevious() {
        return i > 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        if(!hasNext())
            throw new IndexOutOfBoundsException("[Chunk.It] range mismatch: " + iterRange() + " does not include " + i);
        lastReturnedShift = 0;
        return (T) a[offset + (++i)*stride];
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
        return (T) a[offset + (i--) * stride];
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
        a[offset + (i + lastReturnedShift)*stride] = e; // FIXME THIS IS NOT CORRECT !!!
    }
}
