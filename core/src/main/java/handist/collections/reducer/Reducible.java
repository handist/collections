package handist.collections.reducer;

import java.util.function.Function;

import handist.collections.ChunkedList;

public interface Reducible<T> extends Iterable<T> {

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList},
     * using an operation provided by default
     *
     * @param op          specifies the type of reduction operation
     * @param extractFunc defines the value to be reduced
     * @return the value after the reduction has completed
     */
    public default boolean reduce(BoolReducer.Op op, Function<T, Boolean> extractFunc) {
        final BoolReducer reducer = new BoolReducer(op);
        forEach((t) -> {
            if (reducer.reduce(extractFunc.apply(t))) { // the result is determined in the middle.
                return;
            }
        });
        return reducer.value();
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList},
     * using an operation provided by default
     *
     * @param op          specifies the type of reduction operation
     * @param extractFunc defines the value to be reduced
     * @return the value after the reduction has completed
     */
    public default double reduce(DoubleReducer.Op op, Function<T, Double> extractFunc) {
        final DoubleReducer reducer = new DoubleReducer(op);
        forEach(t -> reducer.reduce(extractFunc.apply(t)));
        return reducer.value();
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList},
     * using an operation provided by default
     *
     * @param op          specifies the type of reduction operation
     * @param extractFunc defines the value to be reduced
     * @return the value after the reduction has completed
     */
    public default float reduce(FloatReducer.Op op, Function<T, Float> extractFunc) {
        final FloatReducer reducer = new FloatReducer(op);
        forEach(t -> reducer.reduce(extractFunc.apply(t)));
        return reducer.value();
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList},
     * using an operation provided by default
     *
     * @param op          specifies the type of reduction operation
     * @param extractFunc defines the value to be reduced
     * @return the value after the reduction has completed
     */
    public default int reduce(IntReducer.Op op, Function<T, Integer> extractFunc) {
        final IntReducer reducer = new IntReducer(op);
        forEach(t -> reducer.reduce(extractFunc.apply(t)));
        return reducer.value();
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList},
     * using an operation provided by default
     *
     * @param op          specifies the type of reduction operation
     * @param extractFunc defines the value to be reduced
     * @return the value after the reduction has completed
     */
    public default long reduce(LongReducer.Op op, Function<T, Long> extractFunc) {
        final LongReducer reducer = new LongReducer(op);
        forEach(t -> reducer.reduce(extractFunc.apply(t)));
        return reducer.value();
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList}
     * using the reducer provided as parameter
     *
     * @param <R>     type of the reducer
     * @param reducer reducer to be used to reduce this parameter
     * @return the reducer provided as parameter after the reduction has completed
     */
    public default <R extends Reducer<R, T>> R reduce(R reducer) {
        forEach(t -> reducer.reduce(t));
        return reducer;
    }

    /**
     * Sequentially reduces all the elements contained in this {@link ChunkedList},
     * using an operation provided by default
     *
     * @param op          specifies the type of reduction operation
     * @param extractFunc defines the value to be reduced
     * @return the value after the reduction has completed
     */
    public default short reduce(ShortReducer.Op op, Function<T, Short> extractFunc) {
        final ShortReducer reducer = new ShortReducer(op);
        forEach(t -> reducer.reduce(extractFunc.apply(t)));
        return reducer.value();
    }
}
