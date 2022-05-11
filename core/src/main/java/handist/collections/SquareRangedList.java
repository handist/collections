package handist.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.util.Splitter;

public interface SquareRangedList<T> extends Iterable<T> {

    public static int hashCode(SquareRangedList<?> rlist) {
        int hashCode = 1;
        // code from JavaAPI doc of List
        for (final Object o : rlist) {
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        }
        return hashCode;
    }

    /**
     * Returns a list of each column of this instance.
     *
     * @return a list of each column of this instance.
     */
    RangedList<RangedList<T>> asColumnList();

    /**
     * Returns a list of each row of this instance
     *
     * @return a list of each row of this instance.
     */
    RangedList<RangedList<T>> asRowList();

    /**
     * Indicates if this list contains the provided object. More formally if the
     * list contains at least one object {@code a} such that
     * <code>(a == null) ? o == null : a.equals(o);</code> is true.
     *
     * @param v the object whose presence is to be checked
     * @return {@code true} if the collection contains {@code o}, {@code false}
     *         otherwise
     */
    boolean contains(Object v);

    /**
     * Performs the provided action on every element in the collection
     *
     * @param action the action for each element
     */
    @Override
    void forEach(Consumer<? super T> action);

    /**
     * Performs the provided action on every element in the collection
     *
     * @param action the action for each element
     */
    void forEach(SquareIndexTConsumer<? super T> action);

    /**
     * Performs the provided action on every element in the collection for provided
     * range.
     *
     * @param subrange the range of provided action
     * @param action   the action for each element
     */
    void forEach(SquareRange subrange, Consumer<? super T> action);

    /**
     * Performs the provided action with an index on every element in the collection
     * for provided range.
     *
     * @param subrange the range of provided action
     * @param action   the action for each element
     */
    void forEach(SquareRange subrange, SquareIndexTConsumer<? super T> action);

    /**
     * Performs the provided action with an index on every column in the collection
     * for provided range.
     *
     * @param range        the range of provided action
     * @param columnAction the action for each column
     */
    void forEachColumn(LongRange range, LongTBiConsumer<RangedList<T>> columnAction);

    /**
     * Performs the provided action with an index on every column in the
     * collection..
     *
     * @param columnAction the action for each column
     */
    void forEachColumn(LongTBiConsumer<RangedList<T>> columnAction);

    /**
     * Performs the provided action with an index on every row in the collection for
     * provided range.
     *
     * @param range     the range of provided action
     * @param rowAction the action for each row
     */
    void forEachRow(LongRange range, LongTBiConsumer<RangedList<T>> rowAction);

    /**
     * Performs the provided action with an index on every row in the collection..
     *
     * @param rowAction the action for each row
     */
    void forEachRow(LongTBiConsumer<RangedList<T>> rowAction);

    /**
     * Performs the provided action with {@link SquareSiblingAccessor} on every
     * element in the collection for provided range.
     *
     * @param range  the range of provided action
     * @param action the action for each element
     */
    void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<T>> action);

    /**
     * Returns the value associated with the provided {@code long} indexes.
     *
     * @param index  outer index of the value to return
     * @param index2 inner index of the value to return
     * @return the value associated with this index
     */
    T get(long index, long index2);

    /**
     * Returns the view for the provided column in Square.
     *
     * @param column the column for view
     * @return the {@link RangedList} view of provided column
     */
    RangedList<T> getColumnView(long column);

    /**
     * Obtain the {@link SquareRange} on which this instance is defined.
     *
     * @return the {@link SquareRange} object representing
     */
    SquareRange getRange();

    /**
     * Returns the view for the provided row in Square.
     *
     * @param row the row for view
     * @return the {@link RangedList} view of provide row
     */
    RangedList<T> getRowView(long row);

    /**
     * Returns the view for the provided ranges in Square.
     *
     * @param ranges the {@link SquareRange} for views
     * @return the views of provided {@link SquareRange}.
     */
    default List<SquareRangedList<T>> getViews(List<SquareRange> ranges) {
        final List<SquareRangedList<T>> results = new ArrayList<>();
        ranges.forEach((SquareRange range) -> {
            results.add(subView(range));
        });
        return results;
    }

    @Override
    abstract Iterator<T> iterator();

    /**
     * Checks if the provided {@code long index} point is included in the range this
     * instance is defined on, i.e. if method {@link #get(long, long)}, or
     * {@link #set(long,long,Object)} can be safely called with the provided
     * parameter.
     *
     * @param outer the long value whose represents the outer index of the point
     * @param inner the long value whose represents the inner index of the point
     * @throws IndexOutOfBoundsException if the provided index is outside the range
     *                                   this instance is defined on
     */
    public default void rangeCheck(long outer, long inner) {
        if (!this.getRange().contains(outer, inner)) {
            throw new IndexOutOfBoundsException("[SquareRangedListAbstract] range mismatch: " + this.getRange()
                    + " does not include point(" + outer + "," + inner + ")");
        }
    }

    /**
     * Checks if the provided {@link SquareRange} is included in the range of this
     * instance.
     *
     * @param target LongRange whose inclusion in this instance is to be checked
     * @throws ArrayIndexOutOfBoundsException if the provided {@link SquareRange} is
     *                                        not included in this instance
     */
    public default void rangeCheck(SquareRange target) {
        if (!this.getRange().contains(target)) {
            throw new ArrayIndexOutOfBoundsException(
                    "[SquareRangedListAbstract] range mismatch:" + this.getRange() + " does not include " + target);
        }
    }

    /**
     * Sets the provided value at the specified index.
     *
     * @param index  the outer index
     * @param index2 the inner index
     * @param value  the value to set.
     * @return previous value that was stored at this index, {@code null} if there
     *         was no previous value or the previous value stored was {@code null}
     */
    T set(long index, long index2, T value);

    /**
     * Return the list of {@link SquareRangedList} that split into <em>outer</em> +
     * <em>inner</em> {@link SquareRangedList} instances of equal size (or near
     * equal size if the size of this instance is not divisible.
     *
     * @param outer the number of split of outer loop.
     * @param inner the number of split of outer loop.
     * @return a list of <em>outer</em> * <em>inner</em> {@link SquareRangedList}
     *         instance.
     */
    default List<SquareRangedList<T>> split(int outer, int inner) {
        return getViews(splitRange(outer, inner));
    }

    /**
     * Splits equally squares into <em>outer</em> * <em>inner</em> and squares are
     * divided to list of sizes <em>num</em>.
     *
     * @param outer     the number of split outer range.
     * @param inner     the number ot split inner range.
     * @param num       the number dividing squares to {@link List}.
     * @param randomize if true, splited squares are added in order from begining,
     *                  else, added in random.
     * @return List of list of {@link SquareRangedList}. Size of outer list is
     *         numTherads. Size of inner list is number of {@link SquareRangedList}
     *         for each thread.
     */
    default List<List<SquareRangedList<T>>> splitN(int outer, int inner, int num, boolean randomize) {
        final List<SquareRange> ranges = splitRange(outer, inner);
        if (randomize) {
            Collections.shuffle(ranges);
        }
        final List<List<SquareRangedList<T>>> results = new ArrayList<>();
        final Splitter split = new Splitter(ranges.size(), num);
        for (int i = 0; i < num; i++) {
            final List<SquareRange> assigned = split.getIth(i, ranges);
            results.add(getViews(assigned));
        }
        return results;
    }

    /**
     * Return the list of {@link SquareRange} that split into <em>outer</em> +
     * <em>inner</em> {@link SquareRange} instances of equal size (or near equal
     * size if the size of this instance is not divisible.
     *
     * @param outer the number of split of outer loop.
     * @param inner the number of split of outer loop.
     * @return a list of <em>outer</em> * <em>inner</em> {@link SquareRange}
     *         instance.
     */
    default List<SquareRange> splitRange(int outer, int inner) {
        return getRange().split(outer, inner);
    }

    abstract Iterator<T> subIterator(SquareRange range);

    /**
     * Provides a SquareRangedList of the elements contained in this instance on the
     * specified {@link SquareRange}.
     * <p>
     * If the provided range exceeds the indices contained in this instance the
     * method will return the elements it contains that fit within the provided
     * range.
     *
     * @param range range of indices of which a copy is desired
     * @return a ranged list of the elements contained in this
     *         {@link SquareRangedList} that fit in the provided range.
     */
    SquareRangedList<T> subView(SquareRange range);

    /**
     * Splits equally squares into <em>outer</em> * <em>inner</em> and return some
     * squares for each host equally. Assigned squares are divided to list of sizes
     * nThreads.
     *
     * @param outer      the number of split outer range.
     * @param inner      the number ot split inner range.
     * @param pg         the group between which this ranged list is split
     * @param numThreads the number of threads to whom squares should be assigned to
     * @param seed       seed used to randomly assign squares to hosts, must the
     *                   same on all hosts
     * @return List of list of {@link SquareRangedList}. Size of outer list is
     *         numTherads. Size of inner list is number of {@link SquareRangedList}
     *         for each thread.
     */
    default List<List<SquareRangedList<T>>> teamedSplitNM(int outer, int inner, TeamedPlaceGroup pg, int numThreads,
            long seed) {
        final int numHosts = pg.size();
        final int ithHost = pg.rank();
        final Random rand = new Random(seed);
        final List<SquareRange> ranges = splitRange(outer, inner);
        if (rand != null) {
            Collections.shuffle(ranges, rand);
        }
        final List<List<SquareRangedList<T>>> results = new ArrayList<>();
        final Splitter split = new Splitter(ranges.size(), numHosts);
        final Splitter splitIn = new Splitter(split.ith(ithHost), split.ith(ithHost + 1), numThreads);
        for (int i = 0; i < numThreads; i++) {
            final List<SquareRange> assigned = splitIn.getIth(i, ranges);
            results.add(getViews(assigned));
        }
        return results;
    }

    /**
     * Returns the elements contained in this instance in a one-dimensional array.
     * Ordered by outer and inner loop in {@link SquareRange}.
     *
     * @return array containing the objects contained in this instance
     */
    Object[] toArray();

    /**
     * Returns the elements contained in this instance in a one-dimensional array.
     * Ordered by outer and inner loop in {@link SquareRange}.
     *
     * @param newRange the range of elements to take
     * @return array containing the objects contained in this instance
     */
    Object[] toArray(SquareRange newRange);

    /**
     * Returns the elements contained in this instance in a one-dimensional
     * {@link Chunk}. Ordered by outer and inner loop in {@link SquareRange}.
     *
     * @param newRange the range of elements to take
     * @return chunk containing the objects contained in this instance
     */
    SquareChunk<T> toChunk(SquareRange newRange);

    /**
     * Returns the elements contained in this instance in a one-dimensional
     * {@link List}. Ordered by outer and inner loop in {@link SquareRange}.
     *
     * @param newRange the range of elements to take
     * @return list containing the objects contained in this instance
     */
    List<T> toList(SquareRange newRange);
}
