package handist.collections.function;

/**
 * Packer. Keeps the given raw values for them to be later returned in the same
 * order through {@link PrimitiveInput}.
 */
public interface PrimitiveOutput {
    /**
     * Store a double into the packer
     *
     * @param value the value to record
     */
    public void writeDouble(double value);

    /**
     * Store an int into the packer
     *
     * @param value the value to record
     */
    public void writeInt(int value);

    /**
     * Store a long in to the packer
     *
     * @param value the value to record
     */
    public void writeLong(long value);
}
