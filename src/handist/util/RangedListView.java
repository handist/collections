package handist.util;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class RangedListView<T> extends AbstractCollection<T> implements RangedList<T> {

    private RangedList<T> base;
    protected LongRange range;

    public static <T> RangedListView<T> emptyView() {
        return new RangedListView<>(null, new LongRange(0, 0));
    }

    public RangedListView(RangedList<T> base, LongRange range) {
        this.base = base;
        this.range = range;
    }

    @Override
    public LongRange getRange() {
        return range;
    }

    @Override
    public boolean contains(Object o) {
        for (long i = range.begin; i < range.end; i++) {
            T elem = base.get(i);
            if (o == null ? elem == null : o.equals(elem)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RangedList<T> clone() {
        return cloneRange(range);
    }

    @Override
    public RangedList<T> cloneRange(LongRange newRange) {
        return toChunk(newRange);
    }

    @Override
    public Chunk<T> toChunk(LongRange newRange) {
	return base.toChunk(newRange);
    }

    @Override
    public Object[] toArray() {
	return base.toArray(range);
    }

    @Override
    public Object[] toArray(LongRange newRange) {
	return base.toArray(newRange);
    }

    @Override
    public T get(long index) {
        return base.get(index);
    }

    @Override
    public T set(long index, T v) {
	return base.set(index, v);
    }

    @Override
    public int size() {
        return (int) longSize();
    }

    @Override
    public long longSize() {
        return range.end - range.begin;
    }

    @Override
    public boolean isEmpty() {
        return range.end - 1 <= range.begin;
    }

    private static class It<T> implements Iterator {
        private long i;
        private RangedListView<T> rangedListView;
	private LongRange range;

        public It(RangedListView<T> rangedListView) {
	    this.rangedListView = rangedListView;
	    this.range = rangedListView.getRange();
	    this.i = range.begin - 1;
        }

	public It(RangedListView<T> rangedListView, long i0) {
	    this.rangedListView = rangedListView;
	    this.range = rangedListView.getRange();
	    this.i = i0 - 1;
	}

	@Override
        public boolean hasNext() {
            return i + 1 < range.end;
        }

        @Override
        public T next() {
            return rangedListView.get(++i);
        }

    }

    @Override
    public Iterator<T> iterator() {
        return new It<T>(this);
    }

    @Override
    public Iterator<T> iteratorFrom(long i) {
	return new It<T>(this, i);
    }


    @Override
    public RangedList<T> subList(long begin, long end) {
        long from = Math.max(begin, range.begin);
        long to = Math.min(end, range.end);
	if (from > to) {
	    throw new ArrayIndexOutOfBoundsException();
	}
	if (begin == range.begin && end == range.end) {
	    return this;
	}
        return new RangedListView<T>(base, new LongRange(from, to));
    }

    @Override
    public T first() {
	return base.get(range.begin);
    }

    @Override
    public T last() {
	return base.get(range.end - 1);
    }

    @Override
    public void each(Consumer action) {
	base.each(range, action);
    }

    @Override
    public void each(LongRange range, Consumer action) {
	if (range.begin == this.range.begin &&
	    range.end == this.range.end) {
	    base.each(range, action);
	} else {
	    long from = Math.max(range.begin, this.range.begin);
	    long to = Math.min(range.end, this.range.end);
	    if (from > to) {
		throw new ArrayIndexOutOfBoundsException();
	    }
	    LongRange range2 = new LongRange(from, to);
	    base.each(range2, action);
	}
    }

    public <U> void each(BiConsumer<T, Receiver<U>> action, Receiver<U> receiver) {
        // Consumer<T> c = action;
        each(this.range, t -> action.accept((T) t, receiver));
    }

    public <U> void each(LongRange range, BiConsumer<T, Receiver<U>> action, Receiver<U> receiver) {
        // Consumer<T> c = action;
        each(this.range, t -> action.accept((T) t, receiver));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[" + range + "]");
        int sz = Config.omitElementsToString ? Math.min(size(), Config.maxNumElementsToString) : size();
        long c = 0;
        for (long i = range.begin; i < range.end; i++) {
            if (c++ > 0) {
                sb.append(",");
            }
            sb.append("" + base.get(i));
            if (c == sz) {
                break;
            }
        }
        if (sz < size()) {
            sb.append("...(omitted " + (size() - sz) + " elements)");
        }
	//        sb.append("@" + range.begin + ".." + last() + "]");
        return sb.toString();
    }

    public static void main(String[] args) {
        long i = 10;
        Chunk<Integer> c = new Chunk<>(new LongRange(10 * i, 11 * i));
        System.out.println("prepare:" + c);
        for (long j = 0; j < i; j++) {
	    int v = (int) (10 * i + j);
            System.out.println("set@" + v);
            c.set(10 * i + j, v);
        }
        System.out.println("Chunk :" + c);
        RangedList<Integer> r1 = c.subList(10 * i + 0, 10 * i + 2);
        RangedList<Integer> r2 = c.subList(10 * i + 2, 10 * i + 8);
        RangedList<Integer> r3 = c.subList(10 * i + 8, 10 * i + 9);
        RangedList<Integer> r4 = c.subList(10 * i + 0, 10 * i + 9);
        System.out.println("RangedListView: " + r1);
        System.out.println("RangedListView: " + r2);
        System.out.println("RangedListView: " + r3);
        System.out.println("RangedListView: " + r4);
    }

}
