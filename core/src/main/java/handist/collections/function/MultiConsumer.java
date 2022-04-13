package handist.collections.function;

/**
 * Interface for a consumer which takes 4 parameters
 *
 * @author Patrick Finnerty
 *
 * @param <T> type of the first parameter
 * @param <U> type of the second parameter
 * @param <V> type of the third parameter
 * @param <O> type of the next parameters
 */
public interface MultiConsumer<T, U, V, O> {
    public void accept(T t, U u, V v, O o);
}
