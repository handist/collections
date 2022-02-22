package handist.collections.function;

@FunctionalInterface
public interface TriConsumer<T, U, V> {
    /**
     * Takes the three arguments given as parameter and performs some action on them
     *
     * @param t first parameter
     * @param u second parameter
     * @param v third parameter
     */
    public void accept(T t, U u, V v);
}
