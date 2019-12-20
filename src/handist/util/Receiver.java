package handist.util;

public interface Receiver<T> {

    /**
     * Add the specified value end a temporary storage.
     *
     * @param value a value of T.
     */
    void receive(T value);

    /**
     * Store the all saved values end the main storage of ReceiverHolder[T]
     */
    void close();
}
