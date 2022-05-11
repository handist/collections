package handist.collections;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Entries in this member will be sorted by increasing {@link LongRange#from}
 * and <em>increasing</em> {@link LongRange#to}.
 *
 * TODO : handle not for LongRange but for object range.
 */
public final class LongRangeSet implements NavigableSet<LongRange>, Serializable {

    private static final long serialVersionUID = 1614965449289258856L;

    private final NavigableSet<LongRange> ranges;

    private final AtomicLong totalSize;

    public LongRangeSet() {
        ranges = new ConcurrentSkipListSet<>();
        totalSize = new AtomicLong(0l);
    }

    public LongRangeSet(Collection<LongRange> col) {
        this();
        for (final LongRange r : col) {
            add(r);
        }
    }

    @Override
    public boolean add(LongRange r) {
        LongRange range = floor(r);
        if (range != null && range.to > r.from) {
            return false; // or throw exception
        }
        range = ceiling(r);
        if (range != null && range.from < r.to) {
            return false; // or throw exception
        }
        totalSize.addAndGet(r.size());
        return ranges.add(r);
    }

    @Override
    public boolean addAll(Collection<? extends LongRange> c) {
        boolean ret = false;
        for (final LongRange r : c) {
            if (add(r)) {
                ret = true;
            }
        }
        return ret;
    }

    @Override
    public LongRange ceiling(LongRange e) {
        return ranges.ceiling(e);
    }

    @Override
    public void clear() {
        ranges.clear();
        totalSize.set(0);
    }

    @Override
    public Comparator<? super LongRange> comparator() {
        return ranges.comparator();
    }

    @Override
    public boolean contains(Object o) {
        return ranges.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return ranges.containsAll(c);
    }

    /**
     * @param i an index to check in contains.
     * @return true if this set contains a range that overlaps the specified index.
     */
    public boolean containsIndex(long i) {
        final LongRange floor = floor(new LongRange(i));
        if (floor != null && floor.to > i) {
            return true;
        }
        final LongRange higher = higher(new LongRange(i));
        if (higher != null && higher.from <= i) {
            return true;
        }
        return false;
    }

    @Override
    public Iterator<LongRange> descendingIterator() {
        return ranges.descendingIterator();
    }

    @Override
    public NavigableSet<LongRange> descendingSet() {
        return ranges.descendingSet();
    }

    @Override
    public LongRange first() {
        return ranges.first();
    }

    @Override
    public LongRange floor(LongRange e) {
        return ranges.floor(e);
    }

    /**
     * Returns a range that overlap a given index, or null if this set contains no
     * range overlap the index.
     *
     * @param i an index to get range.
     * @return a range that overlap a given index, or null if this set contains no
     *         range overlap the index.
     */
    public LongRange getOverlap(long i) {
        if (isEmpty()) {
            return null;
        }
        final LongRange point = new LongRange(i);
        LongRange range = floor(point);
        if (range != null && range.contains(i)) {
            return range;
        }
        range = higher(point);
        if (range != null && range.contains(i)) {
            return range;
        }
        return null;
    }

    /**
     * Returns a view of the portion of this set whose elements arestrictly less
     * than to index. The returned set isbacked by this set, so changes in the
     * returned set arereflected in this set, and vice-versa. The returned
     * setsupports all optional set operations that this set supports.
     */
    public SortedSet<LongRange> headSet(long to, boolean overlap) {
        final LongRange toElement = floor(new LongRange(to));
        if (toElement == null) {
            return headSet(new LongRange(to), false); // empty set
        }
        if (overlap) {
            return headSet(toElement, true);
        }
        return headSet(toElement, (toElement.to <= to));
    }

    @Override
    public SortedSet<LongRange> headSet(LongRange toElement) {
        return ranges.headSet(toElement);
    }

    @Override
    public NavigableSet<LongRange> headSet(LongRange toElement, boolean inclusive) {
        return ranges.headSet(toElement, inclusive);
    }

    @Override
    public LongRange higher(LongRange e) {
        return ranges.higher(e);
    }

    /**
     * Returns shallow copy set within a given range. It does not include the range
     * that overlaps with the given range but extends.
     *
     * @param from a from index of subSet
     * @param to   a to index of subSet
     * @return LongRange set within a given range
     */
    public NavigableSet<LongRange> includeSet(long from, long to) {
        final LongRangeSet result = new LongRangeSet();
        if (isEmpty()) {
            return result;
        }
        LongRange range = ceiling(new LongRange(from));

        if (range == null) {
            return result;
        }
        final Iterator<LongRange> iter = tailSet(range, true).iterator();

        while (iter.hasNext() && range.to <= to) {
            result.add(range);
            range = iter.next();
        }
        return result;
    }

    @Override
    public boolean isEmpty() {
        return ranges.isEmpty();
    }

    @Override
    public Iterator<LongRange> iterator() {
        return ranges.iterator();
    }

    @Override
    public LongRange last() {
        return ranges.last();
    }

    @Override
    public LongRange lower(LongRange e) {
        return ranges.lower(e);
    }

    /**
     * Returns shallow copy set overlapping a given range. Includes ranges that
     * overhang with the given range.
     *
     * @param from a from index of subSet
     * @param to   a to index of subSet
     * @return LongRange set ovelapping a given range
     */
    public NavigableSet<LongRange> overlapSet(long from, long to) {
        final LongRangeSet result = new LongRangeSet();
        final LongRange given = new LongRange(from, to);
        if (isEmpty()) {
            return result;
        }
        LongRange range = floor(new LongRange(from));
        if (range == null) {
            range = higher(range);
        }
        final Iterator<LongRange> iter = tailSet(range, true).iterator();
        while (iter.hasNext() && range.from < to) {
            if (range.isOverlapped(given)) {
                result.add(range);
            }
            range = iter.next();
        }
        return result;
    }

    @Override
    public LongRange pollFirst() {
        return ranges.pollFirst();
    }

    @Override
    public LongRange pollLast() {
        return ranges.pollLast();
    }

    @Override
    public boolean remove(Object o) {
        if (ranges.remove(o)) {
            totalSize.addAndGet(-((LongRange) o).size());
            return true;
        }
        return false;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean ret = false;
        for (final Object r : c) {
            if (remove(r)) {
                ret = true;
            }
        }
        return ret;
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean ret = false;
        for (final LongRange r : this) {
            if (!c.contains(r)) {
                ret = remove(r);
            }
        }
        return ret;
    }

    @Override
    public int size() {
        return ranges.size();
    }

    /**
     * A method to split ranges contained this instance with given range. TODO :not
     * supported multi threads splitting.
     *
     * @return ranges inner given range and newly created as a result of split.
     */
    public Collection<LongRange> split(long from, long to) {
        final LongRange low = getOverlap(from);
        if (low == null || low.from == from || from == low.to) {
        } else {
            remove(low);
            add(new LongRange(low.from, from));
            add(new LongRange(from, low.to));
        }
        final LongRange high = getOverlap(to - 1);
        if (high == null || high.from == to || to == high.to) {
        } else {
            remove(high);
            add(new LongRange(high.from, to));
            add(new LongRange(to, high.to));
        }
        return includeSet(from, to);
    }

    @Override
    public NavigableSet<LongRange> subSet(LongRange fromElement, boolean fromInclusive, LongRange toElement,
            boolean toInclusive) {
        return ranges.subSet(fromElement, fromInclusive, toElement, toInclusive);
    }

    @Override
    public SortedSet<LongRange> subSet(LongRange fromElement, LongRange toElement) {
        return ranges.subSet(fromElement, toElement);
    }

    public SortedSet<LongRange> tailSet(long from) {
        return tailSet(from, true);
    }

    public SortedSet<LongRange> tailSet(long from, boolean overlap) {
        final LongRange fromElement = ceiling(new LongRange(from));
        if (fromElement == null) {
            return tailSet(last(), overlap);
        }
        if (fromElement.from == from) {
            return tailSet(fromElement, overlap);
        }
        if (!overlap) {
            return tailSet(fromElement, true);
        }
        final LongRange low = lower(fromElement);
        if (low != null && low.to > from) {
            return tailSet(low, true);
        }
        return tailSet(fromElement, true);
    }

    @Override
    public SortedSet<LongRange> tailSet(LongRange fromElement) {
        return ranges.tailSet(fromElement);
    }

    @Override
    public NavigableSet<LongRange> tailSet(LongRange fromElement, boolean inclusive) {
        return ranges.tailSet(fromElement, inclusive);
    }

    @Override
    public Object[] toArray() {
        return ranges.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return ranges.toArray(a);
    }

    @Override
    public String toString() {
        // TODO
        return ranges.toString();
    }

    public long totalSize() {
        return totalSize.get();
    }

}
