package handist.collections.dist.util;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;

import mpi.MPI;

/**
 * Class in charge of managing the buffers use for MPI communications. Buffers
 * can be obtained by calling method {@link #getByteBuffer(int)}
 *
 * @author Patrick Finnerty
 *
 */
public class BufferFactory {

    static LinkedList<ByteBuffer> buffers = new LinkedList<>();

    /**
     * Obtain a buffer of at least the specified size. If a previously allocated
     * buffer was returned, this buffer will be re-used.
     *
     * @param size the number of bytes that the buffer needs to contain
     * @return a buffer of at least the specified size
     */
    public synchronized static ByteBuffer getByteBuffer(int size) {
        final Iterator<ByteBuffer> it = buffers.iterator();
        while (it.hasNext()) {
            final ByteBuffer b = it.next();
            if (size <= b.capacity()) {
                it.remove();
                b.rewind();
                return b;
            }
        }

        // Could not obtain a buffer, create a new one
        return MPI.newByteBuffer(size);
    }

    public synchronized static void returnByteBuffer(ByteBuffer buffer) {
        buffers.add(buffer);
    }

    /** Private constructor to prevent instance creation */
    private BufferFactory() {
    }
}
