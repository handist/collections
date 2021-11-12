package handist.collections.dist;

import handist.collections.function.PrimitiveInput;
import handist.collections.function.PrimitiveOutput;

/**
 * Internal class used to accumulate raw types into arrays to leverage the more
 * efficient MPI "raw type" reductions.
 *
 * @author Yoshiki Kawanishi
 */
class PrimitiveStream implements PrimitiveOutput, PrimitiveInput {

    double[] doubleArray;
    int[] intArray;
    long[] longArray;

    private int doubleNow;
    private int intNow;
    private int longNow;

    PrimitiveStream(int initialCapacity) {
        doubleArray = new double[initialCapacity];
        intArray = new int[initialCapacity];
        longArray = new long[initialCapacity];

        doubleNow = 0;
        intNow = 0;
        longNow = 0;
    }

    /**
     * If the previous writing will be repeated n-1 more times, call this in
     * advance. Resize each array to <em>n</em> times the previous writing count.
     *
     * @param n Multiply array size by n
     */
    void adjustSize(int n) {
        resizeDoubleArray(doubleNow * n);
        resizeIntArray(intNow * n);
        resizeLongArray(longNow * n);
    }

    /**
     * Checks that all arrays have been completely filled. Throws an
     * {@link IllegalStateException} if all the prepared arrays are not full.
     */
    void checkIsFull() {
        if (doubleNow != doubleArray.length || intNow != intArray.length || longNow != longArray.length) {
            throw new IllegalStateException(
                    "In CachableChunkedList#allreduce pack operations must apply to all elements");
        }
    }

    @Override
    public double readDouble() {
        return doubleArray[doubleNow++];
    }

    @Override
    public int readInt() {
        return intArray[intNow++];
    }

    @Override
    public long readLong() {
        return longArray[longNow++];
    }

    /**
     * Discards all recorded content in this instance. This effectively returns this
     * instance to the state it was in when created.
     */
    void reset() {
        intNow = 0;
        doubleNow = 0;
        longNow = 0;
    }

    private void resizeDoubleArray(int size) {
        final double[] newArray = new double[size];
        System.arraycopy(doubleArray, 0, newArray, 0, doubleNow);
        doubleArray = newArray;
    }

    private void resizeIntArray(int size) {
        final int[] newArray = new int[size];
        System.arraycopy(intArray, 0, newArray, 0, intNow);
        intArray = newArray;
    }

    private void resizeLongArray(int size) {
        final long[] newArray = new long[size];
        System.arraycopy(longArray, 0, newArray, 0, longNow);
        longArray = newArray;
    }

    @Override
    public void writeDouble(double d) {
        if (doubleNow == doubleArray.length) {
            resizeDoubleArray(doubleNow * 2);
        }
        doubleArray[doubleNow] = d;
        doubleNow++;
    }

    @Override
    public void writeInt(int i) {
        if (intNow == intArray.length) {
            resizeIntArray(intNow * 2);
        }
        intArray[intNow] = i;
        intNow++;
    }

    @Override
    public void writeLong(long l) {
        if (longNow == longArray.length) {
            resizeLongArray(longNow * 2);
        }
        longArray[longNow] = l;
        longNow++;
    }
}
