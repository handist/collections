package handist.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import handist.collections.accumulator.Accumulator;
import handist.collections.accumulator.Accumulator.ThreadLocalAccumulator;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.dist.util.Pair;
import handist.collections.function.MultiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.function.TriConsumer;
import handist.collections.util.Splitter;

/**
 * Factory class used to create new product instances and define common methods
 * for products.
 *
 * @author Patrick Finnerty
 *
 * @param <S> type carried by the first operand to the product
 * @param <T> type carried by the second operant to the product, may be
 *            identical to the first
 */
public interface RangedProduct<S, T> {

    public static int hashCode(SquareRangedList<?> rlist) {
        int hashCode = 1;
        // code from JavaAPI doc of List
        for (final Object o : rlist) {
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        }
        return hashCode;
    }

    /**
     * Creates a product between two ranged lists.
     *
     * @param <S>   type contained by the first list
     * @param <T>   type contained by the second list
     * @param outer first operand to the product
     * @param inner second operand to the product
     * @return a product containing all the combination pairs possible from the two
     *         given arguments
     */
    public static <S, T> RangedProduct<S, T> newProd(RangedList<S> outer, RangedList<T> inner) {
        return new SimpleRangedProduct<>(outer, inner);
    }

    /**
     * Creates a product between two ranged lists containing all unique pairs (upper
     * triangle)
     *
     * @param <S>   type contained by the first list
     * @param <T>   type contained by the second list
     * @param outer first operand to the product
     * @param inner second operand to the product
     * @return a product containing all the combination pairs possible from the two
     *         given arguments
     */
    public static <S, T> RangedProduct<S, T> newProdTri(RangedList<S> outer, RangedList<T> inner) {
        return new SimpleRangedProduct<>(outer, inner, true);
    }

    /**
    *
    */
    public static <S, T> RangedProductList<S, T> teamedProdBlock(RangedList<S> outer, RangedList<T> inner,
            int outerSplit, int innerSplit, long seed, TeamedPlaceGroup pg) {
        final RangedProduct<S, T> prod = new SimpleRangedProduct<>(outer, inner);
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     *
     */
    public static <S, T> RangedProductList<S, T> teamedProdCyclic(RangedList<S> outer, RangedList<T> inner,
            int outerSplit, int innerSplit, long seed, TeamedPlaceGroup pg) {
        final RangedProduct<S, T> prod = new SimpleRangedProduct<>(outer, inner);
        // TODO
        throw new UnsupportedOperationException();
    }

    /**
     *
     */
    public static <S, T> RangedProductList<S, T> teamedProdRandom(RangedList<S> outer, RangedList<T> inner,
            int outerSplit, int innerSplit, long seed, TeamedPlaceGroup pg) {
        final RangedProduct<S, T> prod = new SimpleRangedProduct<>(outer, inner);
        return prod.teamedSplit(outerSplit, innerSplit, pg, seed);
    }

    /**
     *
     */
    public static <S, T> RangedProductList<S, T> teamedProdTriBlock(RangedList<S> outer, RangedList<T> inner,
            int outerSplit, int innerSplit, long seed, TeamedPlaceGroup pg) {
        final RangedProduct<S, T> prod = new SimpleRangedProduct<>(outer, inner, true);
        // TODO
        return null;
    }

    /**
     *
     */
    public static <S, T> RangedProductList<S, T> teamedProdTriCyclic(RangedList<S> outer, RangedList<T> inner,
            int outerSplit, int innerSplit, long seed, TeamedPlaceGroup pg) {
        final RangedProduct<S, T> prod = new SimpleRangedProduct<>(outer, inner, true);
        // TODO
        return null;
    }

    /**
     *
     */
    public static <S, T> RangedProductList<S, T> teamedProdTriRandom(RangedList<S> outer, RangedList<T> inner,
            int outerSplit, int innerSplit, long seed, TeamedPlaceGroup pg) {
        final RangedProduct<S, T> prod = new SimpleRangedProduct<>(outer, inner, true);
        return prod.teamedSplit(outerSplit, innerSplit, pg, seed);
    }

    /**
     * Performs the provided action on every element in the collection
     *
     * @param action the action for each element
     */
    void forEach(Consumer<? super Pair<S, T>> action);

    /**
     * Performs the provided action on every element in the collection
     *
     * @param action the action for each element
     */
    void forEach(SquareIndexTConsumer<? super Pair<S, T>> action);

    /**
     * Performs the provided action on every element in the collection for provided
     * range.
     *
     * @param subRange the range of provided action
     * @param action   the action for each element
     */
    void forEach(SquareRange subRange, Consumer<? super Pair<S, T>> action);

    /**
     * Performs the provided action with an index on every element in the collection
     * for provided range.
     *
     * @param subRange the range of provided action
     * @param action   the action for each element
     */
    void forEach(SquareRange subRange, SquareIndexTConsumer<? super Pair<S, T>> action);

    /**
     *
     * @param action action to perform with regards to all the pairs form by the
     *               first argument and the entries contained in the second argument
     */
    void forEachInner(BiConsumer<T, RangedList<S>> action);

    /**
     * Performs the provided action with an index on every row in the collection for
     * provided range.
     *
     * @param range  the range of provided action
     * @param action the action for each row
     */
    void forEachInner(LongRange range, BiConsumer<T, RangedList<S>> action);

    /**
     * Performs the provided action with an index on every column in the
     * collection..
     *
     * @param action the action for each column
     */
    void forEachOuter(BiConsumer<S, RangedList<T>> action);

    /**
     * Performs the provided action with an index on every column in the collection
     * for provided range.
     *
     * @param range  the range of provided action
     * @param action the action for each column
     */
    void forEachOuter(LongRange range, BiConsumer<S, RangedList<T>> action);

    /**
     * Performs the provided action with {@link SquareSiblingAccessor} on every
     * element in the collection for provided range.
     *
     * @param range  the range of provided action
     * @param action the action for each element
     */
    void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<Pair<? super S, ? super T>>> action);

    /**
     * Returns the value associated with the provided {@code long} indexes.
     *
     * @param outerIndex outer index of the value to return
     * @param innerIndex inner index of the value to return
     * @return the value associated with this index
     */
    Pair<S, T> get(long outerIndex, long innerIndex);

    RangedList<T> getInnerPairs(long outerIndex);

    RangedList<S> getOuterPairs(long innerIndex);

    /**
     * Obtain the {@link SquareRange} on which this instance is defined.
     *
     * @return the {@link SquareRange} object representing
     */
    SquareRange getRange();

    /**
     * Returns the view for the provided ranges in Square.
     *
     * @param ranges the {@link SquareRange} for views
     * @return the views of provided {@link SquareRange}.
     */
    default List<RangedProduct<S, T>> getViews(List<SquareRange> ranges) {
        final List<RangedProduct<S, T>> results = new ArrayList<>();
        ranges.forEach((SquareRange range) -> {
            results.add(subView(range));
        });
        return results;
    }

    abstract Iterator<Pair<S, T>> iterator();

    default <A> void parallelForEach(Accumulator<A> acc, BiConsumer<Pair<S, T>, ThreadLocalAccumulator<A>> action) {
        parallelForEach(acc, Runtime.getRuntime().availableProcessors(), action);
    }

    <A> void parallelForEach(Accumulator<A> acc, int parallelism,
            BiConsumer<Pair<S, T>, ThreadLocalAccumulator<A>> action);

    <A, O> void parallelForEachOuter(Accumulator<A> acc, int parallelism,
            MultiConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier);

    <A> void parallelForEachOuter(Accumulator<A> acc, int parallelism,
            TriConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>> action);

    /**
     * Checks if the provided {@code long index} point is included in the range this
     * instance is defined on, i.e. if method {@link #get(long, long)} can be safely
     * called with the provided parameter.
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
     * Return the list of {@link SquareRangedList} that split into <em>outer</em> +
     * <em>inner</em> {@link SquareRangedList} instances of equal size (or near
     * equal size if the size of this instance is not divisible.
     *
     * @param outer the number of split of outer loop.
     * @param inner the number of split of outer loop.
     * @return a list of <em>outer</em> * <em>inner</em> {@link SquareRangedList}
     *         instance.
     */
    default RangedProductList<S, T> split(int outer, int inner) {
        return new RangedProductList<>(this, splitRange(outer, inner));
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

    Iterator<Pair<S, T>> subIterator(SquareRange range);

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
    RangedProduct<S, T> subView(SquareRange range);

    default RangedProductList<S, T> teamedSplit(int outer, int inner, TeamedPlaceGroup pg, long seed) {
        final int numHosts = pg.size();
        final int ithHost = pg.rank();
        final Random rand = new Random(seed);
        final List<SquareRange> ranges = splitRange(outer, inner);
        if (rand != null) {
            Collections.shuffle(ranges, rand);
        }

        final Splitter split = new Splitter(ranges.size(), numHosts);
        final List<SquareRange> filteredRanges = split.getIth(ithHost, ranges);

        return new RangedProductList<>(this, filteredRanges);
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
    SquareChunk<Pair<S, T>> toChunk(SquareRange newRange);

    /**
     * Returns the elements contained in this instance in a one-dimensional
     * {@link List}. Ordered by outer and inner loop in {@link SquareRange}.
     *
     * @param newRange the range of elements to take
     * @return list containing the objects contained in this instance
     */
    List<Pair<S, T>> toList(SquareRange newRange);
}
