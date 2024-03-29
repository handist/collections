/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

/**
 * Class {@link LongRange} describes an interval over {@code long} values.
 * <p>
 * The lower bound is included and the upper bound is excluded from the
 * interval, meaning that for two {@code long} values a and b (a&lt;b), all the
 * {@code long} values l such that a &le; l &lt; b are contained within the
 * {@link LongRange} [a,b).
 * <p>
 * It is possible to create "empty" {@link LongRange} instances where the lower
 * bound is equal to the upper bound. In this case it is considered that there
 * are no {@code long} values included in the {@link LongRange}.
 */
public class LongRange implements Comparable<LongRange>, Iterable<Long>, Serializable {
    /**
     * Iterator on the {@code long} indices contained in a {@link LongRange}
     */
    class It implements Iterator<Long> {
        long current;

        It() {
            current = from;
        }

        @Override
        public boolean hasNext() {
            return current < to;
        }

        @Override
        public Long next() {
            return current++;
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 6430187870603427655L;

    /**
     * Splits the {@link LongRange} provided in the list into <em>n</em> lists of
     * {@link LongRange} instances such that the accumulated size of each list's
     * {@link LongRange} are the same.
     * <p>
     * To achieve this, {@link LongRange} instances may be split into several
     * instances that will placed in different lists.
     * <p>
     * The {@link LongRange} instances given as parameter are not
     *
     * @param n          number of lists of equal sizes
     * @param longRanges {@link LongRange} instances to distribute into the lists
     * @return lists of {@link LongRange} instances of equivalent
     */
    public static List<List<LongRange>> splitList(int n, List<LongRange> longRanges) {
        long totalNum = 0;
        for (final LongRange item : longRanges) {
            totalNum += item.size();
        }
        final long rem = totalNum % n;
        final long quo = totalNum / n;
        final List<List<LongRange>> result = new ArrayList<>(n);
        final Iterator<LongRange> iter = longRanges.iterator();
        LongRange c = iter.next();
        long used = 0;

        for (int i = 0; i < n; i++) {
            final List<LongRange> r = new ArrayList<>();
            result.add(r);
            long rest = quo + ((i < rem) ? 1 : 0);
            while (rest > 0) {
                if (c.size() - used <= rest) {
                    final long from = c.from + used;
                    r.add(new LongRange(from, c.to));
                    rest -= c.size() - used;
                    used = 0;
                    if (!iter.hasNext()) {
                        // Avoids calling iter.next when the last LongRange has
                        // been used. Is necessary due to this "border" case
                        break;
                    }
                    c = iter.next();
                } else {
                    final long from = c.from + used;
                    final long to = from + rest;
                    r.add(new LongRange(from, to));
                    used += rest;
                    rest = 0;
                }
            }
        }
        return result;
    }

    /** Lower bound of the interval (included) */
    public final long from;

    /** Upper bound of the interval (excluded) */
    public final long to;

    /**
     * Constructs an empty LongRange using a single point for a bound. Mainly used
     * for comparison or search.
     *
     * @param index the lower and upper bound of the LongRange to create.
     */
    public LongRange(long index) {
        from = to = index;
    }

    /**
     * Constructs a LongRange with the provided parameters.
     *
     * @param from lower bound of the range (inclusive)
     * @param to   upper bound of the range (exclusive)
     * @throws IllegalArgumentException if the provided lower bound is superior
     *                                  (striclty) to the upper bound
     */
    public LongRange(long from, long to) {
        if (from > to) {
            throw new IllegalArgumentException("Cannot create LongRange from " + from + " to " + to);
        }
        this.from = from;
        this.to = to;
    }

    /**
     * Compares the provided instance to this instance and returns an integer
     * indicating if the provided instance is less than, equal to, or greater than
     * this instance.
     * <p>
     * The implementation relies on ordering the lower bounds first before using the
     * ordering of the upper bounds. The implemented ordering of {@link LongRange}
     * is consistent with equals. To illustrate the ordering, consider the following
     * examples:
     * <ul>
     * <li>[0,0) &lt; [0,100) &lt; [1,1) &lt; [1,20) &lt; [1,21)
     * <li>[0,0) == [0,0)
     * <li>[0,10) == [0,10)
     * </ul>
     * <p>
     *
     * @param r the object to be compared
     * @return a negative integer, zero, or a positive integer as this object is
     *         less than, equal to, or greater than the specified object
     * @throws NullPointerException if the instance given as parameter is null
     */
    @Override
    public int compareTo(LongRange r) {
        // if (to <= r.from && from != to ) {
        // return -1;
        // } else if (r.to <= from && from != to) {
        // return 1;
        // }
        // The LongRange instances overlap,
        // We order them based on "from" first and "to" second
        final int fromComparison = Long.compare(from, r.from);
        return (fromComparison == 0) ? Long.compare(to, r.to) : fromComparison;
    }

    /**
     * Checks if all the indices in this range are included in one of the keys
     * contained by the provided {@code ConcurrentSkipListMap}.
     *
     * @param rmap the ConcurrentSkipListMap instance to check
     * @return a LongRange key of the provided ConcurrentSkipListMap instance that
     *         intersects this instance, or {@code null} if there are so such key.
     */
    public boolean contained(ConcurrentSkipListMap<LongRange, ?> rmap) {
        long current = from;
        while (true) {
            final LongRange tmp = new LongRange(current, current);
            final LongRange result = tmp.findOverlap(rmap);
            if (result == null) {
                break;
            }
            if (result.to >= to) {
                return true;
            }
            current = result.to;
        }
        return false;
    }

    /**
     * Indicates if the provided index is included in this instance. A {@code long}
     * l is contained in a {@link LongRange} [a,b) (a &lt; b) iff a &le; l &lt; b.
     * If the {@link LongRange} has identical lower and upper bound, it does not
     * contain any index.
     *
     * @param index the long value whose inclusion in this instance is to be checked
     * @return {@code true} if the index is included within the bounds of this
     *         {@link LongRange}, {@code false} otherwise
     */
    public boolean contains(long index) {
        return (from <= index) && (index < to);
    }

    /**
     * Indicates if the provided {@link LongRange} is included within this instance.
     * A LongRange is included inside this instance iff its lower bound is greater
     * than or equal to this instance lower bound, and if its upper bound is less
     * than or equal to this instance upper bound.
     *
     * @param range the range whose inclusion into this instance needs to be checked
     * @return true if all the indices of the provided long range are present in
     *         this instance.
     */
    public boolean contains(LongRange range) {
        return (from <= range.from) && (range.to <= to);
    }

    /**
     * Checks whether the provided instance and this instance are equal. Two
     * {@link LongRange} instances are equal if they share the same upper and lower
     * bounds.
     *
     * @return true if the provided instance and this instance are equal
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LongRange)) {
            return false;
        }
        final LongRange range2 = (LongRange) o;
        return from == range2.from && to == range2.to;
    }

    // TODO
    // I cannot find a way to convert ConcurrentSkipListMap to TreeSet (or something
    // having
    // floor/ceiling).
    // (I think TreeSet used ConcurrentSkipListMap in its implementation.)
    // prepare TreeSet version of the following methods
    // OR
    // prepare LongRangeSet having such facilities
    /**
     * Checks if this instance intersects with one of the keys contained by the
     * provided {@code ConcurrentSkipListMap<LongRange, S> rmap}. Returns the
     * smallest of the intersecting keys, or {@code null} if there are no such
     * intersecting key.
     *
     * @param rmap the ConcurrentSkipListMap instance to check
     * @return a LongRange key of the provided ConcurrentSkipListMap instance that
     *         intersects this instance, or {@code null} if there are so such key.
     */
    public LongRange findOverlap(ConcurrentSkipListMap<LongRange, ?> rmap) {
        final LongRange floorKey = rmap.floorKey(this);
        if (floorKey != null && floorKey.isOverlapped(this)) {
            return floorKey;
        }
        final LongRange nextKey = rmap.higherKey(this);
        if (nextKey != null && nextKey.isOverlapped(this)) {
            return nextKey;
        }
        return null;
    }

    /**
     * Calls the provided function with every {@code long} index contained in this
     * instance.
     * <p>
     * Calling this function on empty {@link LongRange} instances will not result in
     * any call to the function.
     *
     * @param func the function to apply with every index of this instance
     */
    public void forEach(LongConsumer func) {
        for (long current = from; current < to; current++) {
            func.accept(current);
        }
    }

    /**
     * Returns a hash code for the {@link LongRange}. The hash-code is generated
     * based on some bit shift operations on the {@link #from lower} and {@link #to
     * upper bound} of the {@link LongRange}.
     *
     * @return hash-code for this instance
     */
    @Override
    public int hashCode() {
        return (int) ((from << 4) + (from >> 16) + to);
    }

    /**
     * Return the intersection range of this instance ad the provided one. If there
     * are no indices that belongs to either ranges, returns null;
     * <p>
     * If either {@code this} or the provided argument are singular point
     * LongRanges, the result will always be {@code null}.
     *
     * @param range the range whose intersection with this instance is to be checked
     * @return a {@link LongRange} representing the intersection between this and
     *         the provided instance, {@code null} if there is no intersection
     */
    public LongRange intersection(LongRange range) {
        final long from = Math.max(range.from, this.from);
        final long to = Math.min(range.to, this.to);
        if (from >= to) {
            return null;
        }
        return new LongRange(from, to);
    }

    /**
     * Returns true if the provided {@link LongRange} and this instance are
     * overlapped. This operation is symmetric, meaning that calling this method
     * with two instances a and b, the result produced by {@code a.isOverlapped(b)}
     * is the same as {@code b.isOverlapped(a)}.
     * <p>
     * Two {@link LongRange} a and b are overlapped if they share some indices, that
     * is if there exist a {@code long} l such that a.contains(l) and b.contains(l)
     * return true.
     * <p>
     * In cases where an empty {@link LongRange} and a non-empty {@link LongRange}
     * are considered, this method returns true if the lower bound (or upper bound
     * as it has the same value) of the empty instance is between the lower bound
     * (included) and the upper bound (excluded) of the other instance.
     * <p>
     * If both considered {@link LongRange} are empty, returns true if they have the
     * same bounds.
     *
     * @param range the range whose overlap with this instance is to be checked
     * @return true if the provided LongRange and this instance overlap
     */
    public boolean isOverlapped(LongRange range) {
        if (equals(range)) {
            return true;
        } else if (from == to) {
            return from >= range.from && from < range.to;
        } else if (range.from == range.to) {
            return range.from >= from && range.from < to;
        } else {
            return (from < range.from) ? (to > range.from) : (from < range.to);
        }
    }

    /**
     * Returns an iterator on the {@code long} indices contained in this instance
     *
     * @return a new iterator starting at {@link #from} and whose last value is the
     *         long preceding {@link #to}
     */
    @Override
    public Iterator<Long> iterator() {
        return new It();
    }

    /**
     * Returns the size of the LongRange, i.e. how many different indices are
     * contained between its lower bound and its upper bound. In practice, returns
     * the difference between {@link #to} and {@link #from}.
     *
     * @return size of the {@link LongRange}
     */
    public long size() {
        return to - from;
    }

    /**
     * Splits the LongRange into <em>n</em> LongRange instances of equal size (or
     * near equal size if the size of this instance is not divisible by <em>n</em>.
     *
     * @param n the number of LongRange instance in which to split this instance
     * @return a list of <em>n</em> consecutive LongRange instances
     */
    public List<LongRange> split(int n) {
        final ArrayList<LongRange> result = new ArrayList<>();
        final long rem = size() % n;
        final long quo = size() / n;
        long c = from;

        for (int i = 0; i < n; i++) {
            final long given = quo + ((i < rem) ? 1 : 0);
            result.add(new LongRange(c, c + given));
            c += given;
        }
        return result;
    }

    /**
     * Streams every {@code long} index contained in this instance.
     *
     * @return a {@link LongStream} of every index contained in this instance
     */
    public LongStream stream() {
        return LongStream.range(from, to);
    }

    /**
     * Returns this LongRange printed in the following format:
     * [lower_bound,upper_bound)
     *
     * @return the range of this {@link LongRange} as "[lower_bound,upper_bound)"
     */
    @Override
    public String toString() {
        return "[" + from + "," + to + ")";
    }
}
