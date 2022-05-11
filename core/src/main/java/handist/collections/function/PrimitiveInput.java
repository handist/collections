package handist.collections.function;

/**
 * Unpacker. Returns items previously packed through {@link PrimitiveOutput} in
 * their raw from.
 */
public interface PrimitiveInput {
    /**
     * Obtains a double from the packed data
     *
     * @return the next double in the packed data
     */
    public double readDouble();

    /**
     * Obtains an integer from the packed data
     *
     * @return the next integer in the packed data
     */
    public int readInt();

    /**
     * Obtains a long from the packed data
     *
     * @return the next long in the packed data
     */
    public long readLong();
}
