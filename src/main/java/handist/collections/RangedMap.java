package handist.collections;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 *
 *
 * TODO : handle not for LongRange but for object range.
 *
 * @author yoshikikawanishi
 */
public class RangedMap<T> extends ParallelMap<Long, T> implements Serializable {

    private static final long serialVersionUID = -977394469515519577L;

    protected final transient ConcurrentNavigableMap<Long, T> data;

    protected final transient LongRangeSet ranges;

    /**
     * Construct a RangedMap with given arguments.
     */
    public RangedMap() {
        this(new ConcurrentSkipListMap<>(), new LongRangeSet());
    }

    /**
     * Construct a RangedMap with given arguments.
     */
    RangedMap(ConcurrentNavigableMap<Long, T> data, Collection<LongRange> ranges) {
        super(data);
        this.data = (ConcurrentNavigableMap<Long, T>) super.data;
        this.ranges = new LongRangeSet(ranges);
    }

    /**
     * Add a range to this instance. You can {@link #put} element within the range
     * added by this method.
     *
     * @param range a range to add to this instance.
     */
    public void addRange(LongRange range) {
        ranges.add(range);
    }

    /**
     * Add a range to this instance. You can {@link #put} element within the range
     * added by this method.
     *
     * @param range       a range to add to this instance.
     * @param initializer generates the initial value of the element for each index.
     */
    public void addRange(LongRange range, Function<Long, T> initializer) {
        ranges.add(range);
        for (final long i : range) {
            put(i, initializer.apply(i));
        }
    }

    /**
     * Remove all ranges and the elements set there.
     */
    @Override
    public void clear() {
        super.clear();
        ranges.clear();
    }

    @Override
    public Object clone() {
        final RangedMap<T> clone = new RangedMap<>();
        for (final LongRange r : ranges) {
            clone.addRange(r);
        }
        for (final Entry<Long, T> e : entrySet()) {
            clone.put(e.getKey(), e.getValue());
        }
        return clone;
    }

    public boolean containsRange(LongRange range) {
        return ranges.contains(range);
    }

    public int count() {
        return data.size();
    }

    public void forEach(LongRange range, BiConsumer<Long, T> func) {
        data.subMap(range.from, range.to).forEach((Long i, T t) -> {
            func.accept(i, t);
        });
    }

    public void forEach(LongRange range, Consumer<T> func) {
        data.subMap(range.from, range.to).forEach((Long i, T t) -> {
            func.accept(t);
        });
    }

    @Override
    public T put(Long key, T value) {
        if (!ranges.containsIndex(key)) {
            throw new IndexOutOfBoundsException("RangedMap: index " + key + " is not included ");
        }
        return super.put(key, value);
    }

    @Override
    public void putAll(java.util.Map<? extends Long, ? extends T> m) {
        m.forEach((key, value) -> {
            this.put(key, value);
        });
    }

    public Collection<LongRange> ranges() {
        return ranges;
    }

    public Map<Long, T> remove(LongRange range) {
        if (ranges.remove(range)) {
            final TreeMap<Long, T> mapToRet = new TreeMap<>();
            data.subMap(range.from, range.to).forEach((Long i, T t) -> {
                data.remove(i);
                mapToRet.put(i, t);
            });
            return mapToRet;
        }
        return null;
    }

    protected boolean removeRangeTemporary(LongRange range) {
        return ranges.remove(range);
    }

    @Override
    public int size() {
        return (int) ranges.totalSize();
    }

    /**
     * Splits this instance by two points of a given range and returns a subMap of
     * the given range.
     *
     * @param range to split
     * @return subMap within a given range.
     */
    public RangedMap<T> split(LongRange range) {
        final LongRange low = ranges.getOverlap(range.from);
        if (low != null && low.from != range.from && low.to != range.from) {
            removeRangeTemporary(low);
            addRange(new LongRange(low.from, range.from));
            addRange(new LongRange(range.from, low.to));
        }
        final LongRange high = ranges.getOverlap(range.to);
        if (high != null && high.from != range.to && high.to != range.to) {
            removeRangeTemporary(high);
            addRange(new LongRange(high.from, range.to));
            addRange(new LongRange(range.to, high.to));
        }
        return new RangedMap<>(data.subMap(range.from, range.to),
                ranges.subSet(new LongRange(range.from), new LongRange(range.to)));
    }

    @Override
    public String toString() {
        // TODO
        return data.toString();
    }
}
