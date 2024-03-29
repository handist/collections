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
import java.util.List;

import handist.collections.function.SquareIndexConsumer;

/**
 * Class {@link SquareRange} describes an interval over {@code long} values.
 * <p>
 * The lower bound is included and the upper bound is excluded from the
 * interval, meaning that for two {@code long} values a and b (a&lt;b), all the
 * {@code long} values l such that a &le; l &lt; b are contained within the
 * {@link SquareRange} [a,b).
 * <p>
 * It is possible to create "empty" {@link SquareRange} instances where the
 * lower bound is equal to the upper bound. In this case it is considered that
 * there are no {@code long} values included in the {@link SquareRange}.
 */
public class SquareRange implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 6430187870603427655L;

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
     * @param n            number of lists of equal sizes
     * @param squareRanges {@link SquareRange} instances to distribute into the
     *                     lists
     * @return lists of {@link SquareRange} instances of equivalent
     */
    public static List<List<SquareRange>> splitList(int n, List<SquareRange> squareRanges) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    /** the range of the first dimension */
    public final LongRange outer;

    /** the range of the second dimension */
    public final LongRange inner;

    /**
     * the shape formed by inner and outer becomes uppertriangle
     */
    boolean isUpperTriangle;
    /** */
    long triangleDiff;

    /**
     * Constructs a LongRange with the provided parameters.
     *
     * @param outer the range of the first range (outer loop)
     * @param inner the range of the second dimension (inner loop#
     * @throws IllegalArgumentException if the range of the first or the second
     *                                  dimension is null
     */
    public SquareRange(LongRange outer, LongRange inner) {
        this.outer = outer;
        this.inner = inner;
        this.isUpperTriangle = false;
        this.triangleDiff = 0l;
    }

    /**
     * Constructs a LongRange with the provided parameters.
     *
     * @param outer           the range of the first range (outer loop)
     * @param inner           the range of the second dimension (inner loop#
     * @param isUpperTriangle if true, the shape formed by inner and outer becomes
     *                        uppertriangle not square. the same index value of
     *                        inner and outer is not included.
     * @throws IllegalArgumentException if the range of the first or the second
     *                                  dimension is null
     */
    public SquareRange(LongRange outer, LongRange inner, boolean isUpperTriangle) {
        this.outer = outer;
        this.inner = inner;
        this.isUpperTriangle = isUpperTriangle;
        this.triangleDiff = 0l;
    }

    /**
     * Constructs a LongRange with the provided parameters.
     *
     * @param outer           the range of the first range (outer loop)
     * @param inner           the range of the second dimension (inner loop#
     * @param isUpperTriangle iif true, the shape formed by inner and outer becomes
     *                        uppertriangle. the same index value of inner and outer
     *                        is not included.
     * @param tri             move the upper triangle down by tri.
     * @throws IllegalArgumentException if the range of the first or the second
     *                                  dimension is null
     */
    public SquareRange(LongRange outer, LongRange inner, boolean isUpperTriangle, long tri) {
        this.outer = outer;
        this.inner = inner;
        this.isUpperTriangle = isUpperTriangle;
        this.triangleDiff = tri;
    }

    /**
     * Returns the range of non-empty columns at the specified row
     *
     * @param row the index of the row within this squared range
     * @return the range of non-empty columns at the specified row
     */
    public LongRange columnRange(long row) {
        return new LongRange(startColumn(row), endColumn(row));
    }

    /**
     * Indicates if the provided index point is included in this instance.
     *
     * @param outer0 the long value whose represents the outer index of the point
     * @param inner0 the long value whose represents the inner index of the point
     * @return {@code true} if the index point is included within the bounds of this
     *         {@link SquareRange}, {@code false} otherwise
     */
    public boolean contains(long outer0, long inner0) {
        return rowRange(inner0).contains(outer0) && columnRange(outer0).contains(inner0);
    }

    /**
     * Indicates if the provided {@link SquareRange} is included within this
     * instance. A SquareRange is included inside the outer and inner ranges of this
     * instance contains the outer and inner ranges of the provided instance
     * respectively.
     *
     * @param range the square range whose inclusion into this instance needs to be
     *              checked
     * @return true if all the indices of the provided long range are present in
     *         this instance.
     */
    public boolean contains(SquareRange range) {
        final long minRow = range.outer.from;
        final long maxRow = range.endRow(range.inner.to - 1) - 1; // care upper triangle
        final long minColumn = range.inner.from;
        final long maxColumn = range.inner.to - 1;
        if (range.isUpperTriangle) {
            return contains(minRow, minColumn) && contains(maxRow, maxColumn);
        } else {
            return contains(maxRow, minColumn) && contains(minRow, maxColumn);
        }
    }

    /**
     * Alternative to {@link #contains(long, long)} which throws an exception is the
     * specified coordinates are not included within this range
     *
     * @param outer0 outer index (row index)
     * @param inner0 inner index (column index)
     * @throws IndexOutOfBoundsException if the specified coordinates are not
     *                                   contained within this square range
     * @see #contains(long, long)
     */
    public void containsCheck(long outer0, long inner0) {
        final boolean result = contains(outer0, inner0);
        if (!result) {
            throw new IndexOutOfBoundsException(
                    "ContainsCheck: " + this + " does not contains [" + outer0 + ", " + inner0 + "].");
        }
    }

    /**
     * Alternative to {@link #contains(SquareRange)} which thrown an exception if
     * the square range of points specified as parameter are not contained within
     * this square range
     *
     * @param range square range of points to check
     * @throws IndexOutOfBoundsException if the specified square range is not
     *                                   contained within this range
     * @see #contains(SquareRange)
     */
    public void containsCheck(SquareRange range) {
        final boolean result = contains(range);
        if (!result) {
            throw new IndexOutOfBoundsException("ContainsCheck: " + this + " does not contains " + range);
        }
    }

    /**
     * Checks if the specified column is present in this square range
     *
     * @param column the column to check
     * @throws IndexOutOfBoundsException if the specified column is not present
     *                                   within this range
     */
    public void containsColumnCheck(long column) {
        final boolean result = inner.contains(column);
        if (!result) {
            throw new IndexOutOfBoundsException("ContainsColumnCheck: " + this + " does not contains column " + column);
        }
    }

    /**
     *
     * @param row the index of the row to check
     * @throws IndexOutOfBoundsException if the specified row is not contained
     *                                   within this square range
     */
    public void containsRowCheck(long row) {
        boolean result = outer.contains(row);
        if (result && isUpperTriangle && startColumn(row) == endColumn(row)) {
            result = false;
        }
        if (!result) {
            throw new IndexOutOfBoundsException("ContainsRowCheck: " + this + " does not contains row " + row);
        }
    }

    /**
     * Returns the index of the last column within this square range
     *
     * @param row the index of the row considered
     * @return the index of the last column included within this range
     */
    public long endColumn(long row) {
        return inner.to;
    }

    /**
     * Returns the last row with values contained within this squared range at the
     * specified column
     *
     * @param column the column considered
     * @return the index of the last row with values within this range at the
     *         specified column
     */
    public long endRow(long column) {
        if (isUpperTriangle) {
            final long row = column + triangleDiff;
            if (row < outer.from) {
                return outer.from;
            } else if (row > outer.to) {
                return outer.to;
            }
            return row;
        }
        return outer.to;
    }

    /**
     * Checks whether the provided instance and this instance are equal. Two
     * {@link SquareRange} instances are equal if they share the same upper and
     * lower bounds.
     *
     * @return true if the provided instance and this instance are equal
     */
    @Override
    public boolean equals(Object o) {
        if (!(o instanceof SquareRange)) {
            return false;
        }
        final SquareRange sqrange2 = (SquareRange) o;
        return inner.equals(sqrange2.inner) && outer.equals(sqrange2.outer)
                && isUpperTriangle == sqrange2.isUpperTriangle && triangleDiff == sqrange2.triangleDiff;
    }

    /**
     * Calls the provided function with every {@code long} index contained in this
     * instance.
     * <p>
     * Calling this function on empty {@link SquareRange} instances will not result
     * in any call to the function.
     *
     * @param func the function to apply with every index of this instance
     */
    public void forEach(SquareIndexConsumer func) {
        rowRange(inner.to - 1).forEach((long i) -> {
            columnRange(i).forEach((long j) -> {
                func.accept(i, j);
            });
        });
    }

    /**
     * Returns a hash code for the {@link SquareRange}. The hash-code is generated
     * based on some bit shift operations on the {@link #outer lower} and
     * {@link #inner upper bound} of the {@link SquareRange}.
     *
     * @return hash-code for this instance
     */
    @Override
    public int hashCode() {
        return ((inner.hashCode() << 4) + (inner.hashCode() >> 16) + outer.hashCode());
    }

    /**
     * Return the intersection range of this instance and the provided one. If there
     * are no index regions that belongs to either ranges, returns null;
     *
     * @param range the square range whose intersection with this instance is to be
     *              checked
     * @return a {@link SquareRange} representing the intersection between this and
     *         the provided instance, {@code null} if there is no intersection
     */
    public SquareRange intersection(SquareRange range) {
        final LongRange interOut = outer.intersection(range.outer);
        final LongRange interInn = inner.intersection(range.inner);
        if (interOut == null || interOut.size() == 0 || interInn == null || interInn.size() == 0) {
            return null;
        }
        final boolean isUpper = this.isUpperTriangle || range.isUpperTriangle;
        if (!isUpper) {
            return new SquareRange(interOut, interInn, false);
        }
        final long triDiff = Math.min(this.triangleDiff, range.triangleDiff);
        return new SquareRange(interOut, interInn, true, triDiff);
    }

    public SquareRange intersectionCheck(SquareRange subrange) {
        containsCheck(subrange);
        // TODO
        // upper rect care...
        return intersection(subrange);
    }

    /**
     * Returns true if the provided {@link SquareRange} and this instance are
     * overlapped. This operation is symmetric, meaning that calling this method
     * with two instances a and b, the result produced by {@code a.isOverlapped(b)}
     * is the same as {@code b.isOverlapped(a)}.
     * <p>
     * Two {@link SquareRange} a and b are overlapped if they share some indices,
     * that is if there exist a {@code long} l such that a.contains(l) and
     * b.contains(l) return true.
     * <p>
     * In cases where an empty {@link SquareRange} and a non-empty
     * {@link SquareRange} are considered, this method returns true if the lower
     * bound (or upper bound as it has the same value) of the empty instance is
     * between the lower bound (included) and the upper bound (excluded) of the
     * other instance.
     * <p>
     * If both considered {@link SquareRange} are empty, returns true if they have
     * the same bounds.
     *
     * @param range the range whose overlap with this instance is to be checked
     * @return true if the provided LongRange and this instance overlap
     */
    public boolean isOverlapped(SquareRange range) {
        if (equals(range)) {
            return true;
        }
        if (!this.inner.isOverlapped(range.inner) || !this.outer.isOverlapped(range.outer)) {
            return false;
        }
        // if the provided range locates the lower triangle of this range
        if (this.isUpperTriangle && this.endRow(range.inner.to - 1) <= range.startRow(range.inner.to - 1)) {
            return false;
        }
        // if this range locates the lower triangle of provided range
        if (range.isUpperTriangle && range.endRow(this.inner.to - 1) <= this.startRow(this.inner.to - 1)) {
            return false;
        }
        return true;
    }

    /**
     * Returns the range of rows that have values within this squared range at the
     * specified column
     *
     * @param column the column considered
     * @return the range of rows included within this squared range that have values
     *         at the specified column
     */
    public LongRange rowRange(long column) {
        return new LongRange(startRow(column), endRow(column));
    }

    /**
     * Returns the size of the this instance.
     *
     * @return size of the {@link SquareRange}
     */
    public long size() {
        if (isUpperTriangle) {
            if (outer.size() < inner.size()) {
                return outer.size() * (inner.size() + (inner.size() - outer.size() + 1)) / 2;
            } else {
                return inner.size() * (inner.size() + 1) / 2;
            }
        } else {
            return inner.size() * outer.size();
        }
    }

    /**
     * Splits this range into a grid with the specified number of
     *
     * @param outerN number of vertical tiles into which to split this range
     * @param innerN number of horizontal tiles into which to split this range
     * @return list of smaller {@link SquareRange}, the union of which covers this
     *         {@link SquareRange}
     */
    public List<SquareRange> split(int outerN, int innerN) {
        // TODO
        // more smart split for upper rectangle
        // lazy way??
        final List<SquareRange> results = new ArrayList<>();
        final List<LongRange> splitOuters = outer.split(outerN);
        final List<LongRange> splitInners = inner.split(innerN);
        for (final LongRange out0 : splitOuters) {
            for (final LongRange in0 : splitInners) {
                final SquareRange sq = intersection(new SquareRange(out0, in0));
                if (sq != null) {
                    results.add(sq);
                }
            }
        }
        return results;
    }

    /**
     * Returns the first column index at the specified row.
     *
     * @param row the index of the row within the square range
     * @return the index of the first non-empty column in this square range
     */
    public long startColumn(long row) {
        if (isUpperTriangle) {
            final long column = row + 1 - triangleDiff;
            if (column > inner.to) {
                return inner.to;
            } else if (column < inner.from) {
                return inner.from;
            }
            return column;
        }
        return inner.from;
    }

    /**
     * Returns the index of the first row with values for the specified column
     *
     * @param column the index of the column considered
     * @return the index of the first row with values at the specified column
     *         contained within this range
     */
    public long startRow(long column) {
        return outer.from;
    }

    /**
     * Returns this SquareRange printed in the following format:
     * [outerRange,innerRange]
     *
     * @return the range of this {@link SquareRange} as "[ outerRange, innerRange] "
     */
    @Override
    public String toString() {
        return "[" + outer + "," + inner + "]" + (isUpperTriangle ? "triangle(" + triangleDiff + ")" : "square");
    }

    /**
     * Returns a {@link SquareRange} that is translated by the provided outer and
     * inner. This method does not change the value of this instance.
     *
     * @param outer translates in the outer direction.
     * @param inner translates in the outer direction.
     * @return translated {@link SquareRange}
     */
    public SquareRange translate(long outer, long inner) {
        return new SquareRange(new LongRange(this.outer.from + outer, this.outer.to + outer),
                new LongRange(this.inner.from + inner, this.inner.to + inner));
    }
}
