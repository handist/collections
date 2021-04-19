package handist.collections.dist;

/**
 * The collection that implements this interface has feature to locate their elements.
 * For example, DistCol manages their elements by their index ranges and DistIdMap manages their element by their keys.
 *
 * @param <T> The type of index or keys to manage the elements.
 */
public interface ElementLocationManagable<T> {
    /**
     * Conduct element location management process.
     * This method must be called simultaneously by process group members.
     */
    public void updateDist();

    /**
     * Computes and gathers the size of each local collection into the provided
     * array.
     * In the case of {@code ElementLocationManagable}, this method is conducted based on the information gathered by the previous {@code updateDist()}.
     *
     * @param result the array in which the result will be stored
     */
    public void getSizeDistribution(final long[] result);
}
