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

import handist.collections.function.SquareIndexConsumer;

import java.awt.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

/**
 * Class {@link SquareRange} describes an interval over {@code long} values.
 * <p>
 * The lower bound is included and the upper bound is excluded from the
 * interval, meaning that for two {@code long} values a and b (a&lt;b), all the
 * {@code long} values l such that a &le; l &lt; b are contained within the
 * {@link SquareRange} [a,b).
 * <p>
 * It is possible to create "empty" {@link SquareRange} instances where the lower
 * bound is equal to the upper bound. In this case it is considered that there
 * are no {@code long} values included in the {@link SquareRange}.
 */
public class SquareRange implements /* Comparable<SquareRange>, Iterable<LongRange>,*/ Serializable {
//    /**
//     * Iterator on the {@code LongRange} contained in a {@link SquareRange}
//     */
//    //class It implements Iterator<Long> {
//    //TK: needless?? Iterable<Pair<Long,LongRange>>??

    /** Serial Version UID */
//    private static final long serialVersionUID = 6430187870603427655L;

    /**
     * Splits the {@link SquareRange} provided in the list into <em>n</em> lists of
     * {@link SquareRange} instances such that the accumulated size of each list's
     * {@link SquareRange} are the same.
     * <p>
     * To achieve this, {@link SquareRange} instances may be split into several
     * instances that will placed in different lists.
     * <p>
     * The {@link SquareRange} instances given as parameter are not
     *
     * @param n          number of lists of equal sizes
     * @param squareRanges {@link SquareRange} instances to distribute into the lists
     * @return lists of {@link SquareRange} instances of equivalent
     */
    public static List<List<SquareRange>> splitList(int n, List<SquareRange> squareRanges) {
        throw new UnsupportedOperationException("not implemented yet") ;
    }

    /** the range of the first dimension */
    public final LongRange outer;

    /** the range of the second dimension */
    public final LongRange inner;

    // TODO more variations...
    boolean isUpperTriangle;
    long triangleDiff;


    /**
     * Constructs a LongRange with the provided parameters.
     *
     * @param outer the range of the first range (outer loop)
     * @param inner the range of the second dimension (inner loop#
     * @throws IllegalArgumentException if the range of the first or the second dimension is null
     */
    public SquareRange(LongRange outer, LongRange inner) {
        this.outer = outer;
        this.inner = inner;
        this.isUpperTriangle = false;
    }
    public SquareRange(LongRange outer, LongRange inner, boolean isUpperTriangle) {
        this.outer = outer;
        this.inner = inner;
        this.isUpperTriangle = isUpperTriangle;
        this.triangleDiff = inner.from - outer.from;
    }
    private SquareRange(LongRange outer, LongRange inner, boolean isUpperTriangle, long tri) {
        this.outer = outer;
        this.inner = inner;
        this.isUpperTriangle = isUpperTriangle;
        this.triangleDiff = tri;
    }

    /**
     * the start column index of the specified row.
     * @param row the index value of the row
     * @return
     */
    public long startColumn(long row) {
        if(isUpperTriangle) return Math.max(row + triangleDiff + 1, inner.from);
        return inner.from;
    }
    public long endColumn(long row) {
        return inner.to;
    }
    public LongRange columnRange(long row) {
        return new LongRange(startColumn(row), endColumn(row));
    }
    public long startRow(long column) {
        return outer.from;
    }
    public long endRow(long column) {
        if(isUpperTriangle) return Math.min(column - triangleDiff, outer.to);
        return outer.to;
    }
    public LongRange rowRange(long column) {
        return new LongRange(startRow(column), endRow(column));
    }


//    /**
//     * TK: needless??
//     * Compares the provided instance to this instance and returns an integer
//     * indicating if the provided instance is less than, equal to, or greater than
//     * this instance.
//     * <p>
//     * The implementation relies on ordering the lower bounds first before using the
//     * ordering of the upper bounds. The implemented ordering of {@link SquareRange}
//     * is consistent with equals. To illustrate the ordering, consider the following
//     * examples:
//     * <ul>
//     * <li>[0,0) &lt; [0,100) &lt; [1,1) &lt; [1,20) &lt; [1,21)
//     * <li>[0,0) == [0,0)
//     * <li>[0,10) == [0,10)
//     * </ul>
//     * <p>
//     *
//     * @param r the object to be compared
//     * @return a negative integer, zero, or a positive integer as this object is
//     *         less than, equal to, or greater than the specified object
//     * @throws NullPointerException if the instance given as parameter is null
//     */
//    @Override
//    public int compareTo(SquareRange r) {
//        // if (to <= r.from && from != to ) {
//        // return -1;
//        // } else if (r.to <= from && from != to) {
//        // return 1;
//        // }
//        // The LongRange instances overlap,
//        // We order them based on "from" first and "to" second
//        final int fromComparison = Long.compare(from, r.from);
//        return (fromComparison == 0) ? Long.compare(to, r.to) : fromComparison;
//    }

//    /**
//     * Checks if all the indices in this range are included in one of the keys
//     * contained by the provided {@code ConcurrentSkipListMap}.
//     *
//     * @param rmap the ConcurrentSkipListMap instance to check
//     * @return a LongRange key of the provided ConcurrentSkipListMap instance that
//     *         intersects this instance, or {@code null} if there are so such key.
//     */
//    public boolean contained(ConcurrentSkipListMap<SquareRange, ?> rmap) {
//        throws new UnsupportedOperationException("not implmented yet");
//    }

    /**
     * Indicates if the provided index point is included in this instance.
     *
     * @param outer0 the long value whose represents the outer index of the point
     * @param inner0 the long value whose represents the inner index of the point
     * @return {@code true} if the index point is included within the bounds of this
     *         {@link SquareRange}, {@code false} otherwise
     */
    public boolean contains(long outer0, long inner0) {
       return outer.contains(outer0) && columnRange(outer0).contains(inner0);
    }
    public void containsCheck(long outer0, long inner0) {
        boolean result = contains(outer0, inner0);
        if(!result) throw new IndexOutOfBoundsException("ContainsCheck: " + this + " does not contains [" + outer0 +", "+ inner0 + "].");
    }


    /**
     * Indicates if the provided {@link SquareRange} is included within this instance.
     * A SquareRange is included inside the outer and inner ranges of this instance
     * contains the outer and inner ranges of the provided instance respectively.
     *
     * @param range the square range whose inclusion into this instance needs to be checked
     * @return true if all the indices of the provided long range are present in
     *         this instance.
     */
    public boolean contains(SquareRange range) {
        if(range.isUpperTriangle || this.isUpperTriangle) throw new UnsupportedOperationException("not implemented yet");
        return outer.contains(range.outer) && inner.contains(range.inner);
    }
    public void containsCheck(SquareRange range) {
        boolean result = contains(range);
        if(!result) throw new IndexOutOfBoundsException("ContainsCheck: " + this + " does not contains " + range);
    }
    public void containsRowCheck(long row) {
        if(this.isUpperTriangle) throw new UnsupportedOperationException("not implemented yet");
        boolean result = outer.contains(row);
        if(!result) throw new IndexOutOfBoundsException("ContainsRowCheck: " + this + " does not contains row " + row);
    }
    public void containsColumnCheck(long column) {
        if(this.isUpperTriangle) throw new UnsupportedOperationException("not implemented yet");
        boolean result = inner.contains(column);
        if(!result) throw new IndexOutOfBoundsException("ContainsColumnCheck: " + this + " does not contains column " + column);
    }



    /**
     * Checks whether the provided instance and this instance are equal. Two
     * {@link SquareRange} instances are equal if they share the same upper and lower
     * bounds.
     *
     * @return true if the provided instance and this instance are equal
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SquareRange)) {
            return false;
        }
        final SquareRange sqrange2 = (SquareRange) o;
        return inner.equals(sqrange2.inner) && outer.equals(sqrange2.outer) &&
                isUpperTriangle==sqrange2.isUpperTriangle && triangleDiff == sqrange2.triangleDiff;
    }

    // TODO
    // I cannot find a way to convert ConcurrentSkipListMap to TreeSet (or something
    // having
    // floor/ceiling).
    // (I think TreeSet used ConcurrentSkipListMap in its implementation.)
    // prepare TreeSet version of the following methods
    // OR
    // prepare LongRangeSet having such facilities
//    /**
//     * Checks if this instance intersects with one of the keys contained by the
//     * provided {@code ConcurrentSkipListMap<LongRange, S> rmap}. Returns the
//     * smallest of the intersecting keys, or {@code null} if there are no such
//     * intersecting key.
//     *
//     * @param rmap the ConcurrentSkipListMap instance to check
//     * @return a LongRange key of the provided ConcurrentSkipListMap instance that
//     *         intersects this instance, or {@code null} if there are so such key.
//     */
//    public SquareRange findOverlap(ConcurrentSkipListMap<SquareRange, ?> rmap) {
//        final SquareRange floorKey = rmap.floorKey(this);
//        if (floorKey != null && floorKey.isOverlapped(this)) {
//            return floorKey;
//        }
//        final SquareRange nextKey = rmap.higherKey(this);
//        if (nextKey != null && nextKey.isOverlapped(this)) {
//            return nextKey;
//        }
//        return null;
//    }

    /**
     * Calls the provided function with every {@code long} index contained in this
     * instance.
     * <p>
     * Calling this function on empty {@link SquareRange} instances will not result in
     * any call to the function.
     *
     * @param func the function to apply with every index of this instance
     */
    public void forEach(SquareIndexConsumer func) {
        outer.forEach((long i)->{
            columnRange(i).forEach((long j)->{
                func.accept(i, j);
            });
        });
    }

    /**
     * Returns a hash code for the {@link SquareRange}. The hash-code is generated
     * based on some bit shift operations on the {@link #outer lower} and {@link #inner
     * upper bound} of the {@link SquareRange}.
     *
     * @return hash-code for this instance
     */
    @Override
    public int hashCode() {
        return  ((inner.hashCode() << 4) + (inner.hashCode() >> 16) + outer.hashCode());
    }



    /**
     * Return the intersection range of this instance and the provided one. If there
     * are no index regions that belongs to either ranges, returns null;
     *
     * @param range the square range whose intersection with this instance is to be checked
     * @return a {@link SquareRange} representing the intersection between this and
     *         the provided instance, {@code null} if there is no intersection
     */
    public SquareRange intersection(SquareRange range) {
        LongRange interOut = outer.intersection(range.outer);
        LongRange interInn = inner.intersection(range.inner);
        if (interOut == null || interOut.size()==0 || interInn == null || interInn.size()==0) {
            return null;
        }
        boolean isUpper = this.isUpperTriangle || range.isUpperTriangle;
        if(!isUpper) return new SquareRange(interOut, interInn, false);
        long triDiff =
                isUpperTriangle?
                        (range.isUpperTriangle? Math.max(triangleDiff, range.triangleDiff): triangleDiff): // TODO min will be used for lowerTriangle
                        range.triangleDiff;
        return new SquareRange(interOut, interInn, isUpper, triDiff).normalizeTriangle();
    }
    private SquareRange normalizeTriangle() {
        if(startColumn(outer.from) >= inner.to) return null;
        if(startColumn(outer.to) < inner.from) return new SquareRange(inner, outer); // normal rec
        return new SquareRange(rowRange(inner.to), columnRange(outer.from-1), true, triangleDiff);
    }
    public SquareRange intersectionCheck(SquareRange subrange) {
        containsCheck(subrange);
        // TODO
        // upper rect care...
        return intersection(subrange);
    }




//    /**
//     * Returns true if the provided {@link SquareRange} and this instance are
//     * overlapped. This operation is symmetric, meaning that calling this method
//     * with two instances a and b, the result produced by {@code a.isOverlapped(b)}
//     * is the same as {@code b.isOverlapped(a)}.
//     * <p>
//     * Two {@link SquareRange} a and b are overlapped if they share some indices, that
//     * is if there exist a {@code long} l such that a.contains(l) and b.contains(l)
//     * return true.
//     * <p>
//     * In cases where an empty {@link SquareRange} and a non-empty {@link SquareRange}
//     * are considered, this method returns true if the lower bound (or upper bound
//     * as it has the same value) of the empty instance is between the lower bound
//     * (included) and the upper bound (excluded) of the other instance.
//     * <p>
//     * If both considered {@link SquareRange} are empty, returns true if they have the
//     * same bounds.
//     *
//     * @param range the range whose overlap with this instance is to be checked
//     * @return true if the provided LongRange and this instance overlap
//     */
//    public boolean isOverlapped(SquareRange range) {
//        if (equals(range)) {
//            return true;
//        } else if (from == to) {
//            return from >= range.from && from < range.to;
//        } else if (range.from == range.to) {
//            return range.from >= from && range.from < to;
//        } else {
//            return (from < range.from) ? (to > range.from) : (from < range.to);
//        }
//    }

//    /**
//     * Returns an iterator on the {@code long} indices contained in this instance
//     *
//     * @return a new iterator starting at {@link #from} and whose last value is the
//     *         long preceding {@link #to}
//     */
//    @Override
//    public Iterator<Long> iterator() {
//        return new It();
//    }

//    /**
//     * Returns the size of the LongRange, i.e. how many different indices are
//     * contained between its lower bound and its upper bound. In practice, returns
//     * the difference between {@link #to} and {@link #from}.
//            *
//            * @return size of the {@link SquareRange}
//     */
//    public long size() {
//        return to - from;
//    }

//    /**
//     * Splits the LongRange into <em>n</em> LongRange instances of equal size (or
//     * near equal size if the size of this instance is not divisible by <em>n</em>.
//     *
//     * @param n the number of LongRange instance in which to split this instance
//     * @return a list of <em>n</em> consecutive LongRange instances
//     */
//    public List<SquareRange> split(int n) {
//        final ArrayList<SquareRange> result = new ArrayList<>();
//        final long rem = size() % n;
//        final long quo = size() / n;
//        long c = from;
//
//        for (int i = 0; i < n; i++) {
//            final long given = quo + ((i < rem) ? 1 : 0);
//            result.add(new SquareRange(c, c + given));
//            c += given;
//        }
//        return result;
//    }

//    /**
//     * Streams every {@code long} index contained in this instance.
//     *
//     * @return a {@link LongStream} of every index contained in this instance
//     */
//    public LongStream stream() {
//        return LongStream.range(from, to);
//    }
    public Collection<SquareRange> split(int outerN, int innerN) {
        // TODO
        // more smart split for upper rectangle
        // lazy way??
        Collection<SquareRange> results = new ArrayList<>();
        List<LongRange> splitOuters = outer.split(outerN);
        List<LongRange> splitInners = inner.split(innerN);
        for (LongRange out0 : splitOuters) {
            for (LongRange in0 : splitInners) {
                SquareRange sq = intersection(new SquareRange(out0, in0));
                if(sq!=null) results.add(sq);
            }
        }
        return results;
    }

    /**
     * Returns this SquareRange printed in the following format:
     * [ outerRange, innerRange ]
     *
     * @return the range of this {@link SquareRange} as "[ outerRange, innerRange] "
     */
    @Override
    public String toString() {
        return "[" + outer + "," + inner + "]";
    }

}
