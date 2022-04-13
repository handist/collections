package handist.collections;

import java.util.function.BiConsumer;
import java.util.function.Function;

import handist.collections.accumulator.Accumulator;
import handist.collections.accumulator.Accumulator.ThreadLocalAccumulator;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.dist.util.Pair;
import handist.collections.function.MultiConsumer;
import handist.collections.function.TriConsumer;

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
public abstract class RangedListProduct<S, T> implements AbstractSquareRangedList<Pair<S, T>, RangedListProduct<S, T>> {

    /**
     * Creates a product between two ranged lists.
     *
     * @param <S>    type contained by the first list
     * @param <T>    type contained by the second list
     * @param first  first operand to the product
     * @param second second operand to the product
     * @return a product containing all the combination pairs possible from the two
     *         given arguments
     */
    public static <S, T> RangedListProduct<S, T> newProduct(RangedList<S> first, RangedList<T> second) {
        return new SimpleRangedListProduct<>(first, second);
    }

    /**
     * Creates a product between two ranged lists containing all unique pairs (upper
     * triangle)
     *
     * @param <S>    type contained by the first list
     * @param <T>    type contained by the second list
     * @param first  first operand to the product
     * @param second second operand to the product
     * @return a product containing all the combination pairs possible from the two
     *         given arguments
     */
    public static <S, T> RangedListProduct<S, T> newProductTriangle(RangedList<S> first, RangedList<T> second) {
        return new SimpleRangedListProduct<>(first, second, true);
    }

    /**
     * Contains is not supported by RangedListProduct.
     */
    @Deprecated
    @Override
    public boolean contains(Object v) {
        throw new UnsupportedOperationException("contains is not supported by RangedListProduct.");
    }

    /**
     *
     * @param action action to perform with regards to all the pairs form by the
     *               first argument and the entries contained in the second argument
     */
    public abstract void forEachRow(BiConsumer<S, RangedList<T>> action);

    public <A> void parallelForEach(Accumulator<A> acc, BiConsumer<Pair<S, T>, ThreadLocalAccumulator<A>> action) {
        parallelForEach(acc, Runtime.getRuntime().availableProcessors(), action);
    }

    public abstract <A> void parallelForEach(Accumulator<A> acc, int parallelism,
            BiConsumer<Pair<S, T>, ThreadLocalAccumulator<A>> action);

    public abstract <A, O> void parallelForEachRow(Accumulator<A> acc, int parallelism,
            MultiConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier);

    public abstract <A> void parallelForEachRow(Accumulator<A> acc, int parallelism,
            TriConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>> action);

    public abstract RangedListProduct<S, T> teamedSplit(int outer, int inner, TeamedPlaceGroup pg, long seed);
}
