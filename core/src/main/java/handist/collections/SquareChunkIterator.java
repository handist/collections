package handist.collections;

import java.util.Iterator;

/**
 * Iterate List that managed {@link SquareRange}. Ordered by the outer and inner
 * loop in {@link SquareRange}.
 */
public class SquareChunkIterator<T> implements Iterator<T> {

    // FIXME not work correctly

    private final Object[] a;
    /** range of target SquareChunk. translate Square that begin (0, 0) */
    private final SquareRange range;
    /** range to iterate */
    private final SquareRange iterateRange;
    // offset
    private int outer;
    private int inner;
    private int i;

    public SquareChunkIterator(SquareRange range, Object[] a) {
        this.a = a;
        this.range = range.translate(-range.outer.from, -range.inner.from); // translate Square that begin (0, 0)
        this.iterateRange = this.range;
        this.outer = (int) this.iterateRange.outer.from;
        this.inner = (int) this.iterateRange.inner.from - 1;
        this.i = (int) (outer * range.inner.size() + inner);
    }

    public SquareChunkIterator(SquareRange iterateRange, SquareRange range, Object[] a) {
        this.a = a;
        this.range = range.translate(-range.outer.from, -range.inner.from); // translate Square that begin (0, 0)
        this.iterateRange = iterateRange.translate(-range.outer.from, -range.inner.from);
        this.outer = (int) this.iterateRange.outer.from;
        this.inner = (int) this.iterateRange.inner.from - 1;
        this.i = (int) (outer * range.inner.size() + inner);
    }

    @Override
    public boolean hasNext() {
        if (inner + 1 < iterateRange.inner.to) {
            return true;
        } else if (outer + 1 < iterateRange.outer.to) {
            return true;
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T next() {
        if (!hasNext()) {
            throw new IndexOutOfBoundsException("[SquareChunkIterator] iteration error");
        }
        if (++inner < iterateRange.inner.to) {
            return (T) a[++i];
        } else {
            inner = (int) iterateRange.inner.from;
            outer++;
            i += 1 + (range.inner.to - iterateRange.inner.to) + iterateRange.inner.from;
            return (T) a[i];
        }

    }

}
