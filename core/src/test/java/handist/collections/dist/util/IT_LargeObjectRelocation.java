package handist.collections.dist.util;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.Place;
import handist.collections.dist.DistBag;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;
import mpi.MPI;
import mpi.MPIException;

/**
 * Test class specifically created to check the capability of our library to
 * handle large object transfers between processes.
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_LargeObjectRelocation implements Serializable {

    public static class DummyObject implements Serializable {
        private static final long serialVersionUID = 6339588383387569811L;
        public final int val;

        public DummyObject(int value) {
            val = value;
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 4330780505661236637L;

    static final int SEED = 42;
    static final int BYTE_COUNT = Integer.MAX_VALUE / 4;

    TeamedPlaceGroup WORLD;

    Place ROOT;

    /**
     * This test is here to check the limits of
     * {@link handist.collections.dist.CollectiveRelocator.Gather}. In the original
     * implementation based solely on arrays, a limitation on the number of bytes
     * that can be exchanged appeared after switching to the OpenMPI Java bindings.
     * This test is here to reproduce this limitation and to confirm that after the
     * switch over to buffers solved this issue.
     */
    @Test(timeout = 20000)
    public void collectiveRelocationGather() {
        final DistBag<DummyObject> col = new DistBag<>(WORLD);

        final int objectPerChunk = 1000;
        final int nbOfChunk = 1000; // make it 10000 to fail with arrays
        final long objPerPlace = objectPerChunk * nbOfChunk;

        WORLD.broadcastFlat(() -> {
            // Adding DummyObjects to the distributed collection
            final long startIndex = WORLD.rank() * objPerPlace;
            final long endIndex = startIndex + objPerPlace;
            final Consumer<DummyObject> consumer = col.getReceiver();
            for (long i = startIndex; i < endIndex; i++) {
                consumer.accept(new DummyObject((int) i));
            }

            // Gathering all the instances on place 0 with a collective relocator
            col.team().gather(ROOT);

            // Checking that every place contains the expected number of objects
            final long expectedLocalCount = here().equals(ROOT) ? 4 * objPerPlace : 0l;
            assertEquals(expectedLocalCount, col.size());
        });

    }

    @SuppressWarnings("deprecation")
    @Test(timeout = 20000)
    public void gatherByteArrayBufferHybrid() {
        WORLD.broadcastFlat(() -> {
            final Random r = new Random(SEED);
            final byte[] buffer = new byte[BYTE_COUNT];
            ByteBuffer rBuf = null;

            // Fill the buffer with bytes
            for (int i = 0; i < BYTE_COUNT; i++) {
                buffer[i] = (byte) r.nextInt();
            }

            // Make a gather call with buffers
            if (here().equals(ROOT)) {
                rBuf = MPI.newByteBuffer(WORLD.size() * BYTE_COUNT);
                WORLD.comm.gather(buffer, BYTE_COUNT, MPI.BYTE, rBuf, BYTE_COUNT, MPI.BYTE, 0);
            } else {
                WORLD.comm.gather(buffer, BYTE_COUNT, MPI.BYTE, 0);
            }

            if (here().equals(ROOT)) {
                final byte[] firstSender = new byte[BYTE_COUNT];
                final byte[] secondSender = new byte[BYTE_COUNT];
                final byte[] thirdSender = new byte[BYTE_COUNT];
                final byte[] fourthSender = new byte[BYTE_COUNT];
                rBuf.get(firstSender);
                rBuf.get(secondSender);
                rBuf.get(thirdSender);
                rBuf.get(fourthSender);

                final Random r2 = new Random(SEED);

                for (int i = 0; i < BYTE_COUNT; i++) {
                    final byte expected = (byte) r2.nextInt();
                    assertEquals(expected, firstSender[i]);
                    assertEquals(expected, secondSender[i]);
                    assertEquals(expected, thirdSender[i]);
                    assertEquals(expected, fourthSender[i]);
                }
            }
        });
    }

    /*
     * This test performed with normal arrays fails to complete within the allocated
     * time. It is therefore Ignored for now
     */
    @SuppressWarnings("deprecation")
    @Ignore
    @Test(timeout = 30000)
    public void gatherByteWithArrays() {
        WORLD.broadcastFlat(() -> {
            final Random r = new Random(SEED);
            final byte[] buffer = new byte[BYTE_COUNT];
            byte[] rBuf = null;

            // Fill the buffer with bytes
            for (int i = 0; i < BYTE_COUNT; i++) {
                buffer[i] = (byte) r.nextInt();
            }

            // Make a gather call with buffers
            if (here().equals(ROOT)) {
                rBuf = new byte[WORLD.size() * BYTE_COUNT];
                WORLD.comm.gather(buffer, BYTE_COUNT, MPI.BYTE, rBuf, BYTE_COUNT, MPI.BYTE, 0);
            } else {
                WORLD.comm.gather(buffer, BYTE_COUNT, MPI.BYTE, 0);
            }

            if (here().equals(ROOT)) {
                final byte[] firstSender = Arrays.copyOfRange(rBuf, 0, BYTE_COUNT);
                final byte[] secondSender = Arrays.copyOfRange(rBuf, BYTE_COUNT, 2 * BYTE_COUNT);
                final byte[] thirdSender = Arrays.copyOfRange(rBuf, 2 * BYTE_COUNT, 3 * BYTE_COUNT);
                final byte[] fourthSender = Arrays.copyOfRange(rBuf, 3 * BYTE_COUNT, 4 * BYTE_COUNT);

                final Random r2 = new Random(SEED);

                for (int i = 0; i < BYTE_COUNT; i++) {
                    final byte expected = (byte) r2.nextInt();
                    assertEquals(expected, firstSender[i]);
                    assertEquals(expected, secondSender[i]);
                    assertEquals(expected, thirdSender[i]);
                    assertEquals(expected, fourthSender[i]);
                }
            }
        });
    }

    @SuppressWarnings("deprecation")
    @Test(timeout = 20000)
    public void gatherByteWithBuffers() {
        WORLD.broadcastFlat(() -> {
            final Random r = new Random(SEED);
            final ByteBuffer buffer = MPI.newByteBuffer(BYTE_COUNT);
            ByteBuffer rBuf = null;

            // Fill the buffer with bytes
            for (int i = 0; i < BYTE_COUNT; i++) {
                buffer.put((byte) r.nextInt());
            }

            // Make a gather call with buffers
            if (here().equals(ROOT)) {
                rBuf = MPI.newByteBuffer(WORLD.size() * BYTE_COUNT);
                WORLD.comm.gather(buffer, BYTE_COUNT, MPI.BYTE, rBuf, BYTE_COUNT, MPI.BYTE, 0);
            } else {
                WORLD.comm.gather(buffer, BYTE_COUNT, MPI.BYTE, 0);
            }

            if (here().equals(ROOT)) {
                final byte[] firstSender = new byte[BYTE_COUNT];
                final byte[] secondSender = new byte[BYTE_COUNT];
                final byte[] thirdSender = new byte[BYTE_COUNT];
                final byte[] fourthSender = new byte[BYTE_COUNT];
                rBuf.get(firstSender);
                rBuf.get(secondSender);
                rBuf.get(thirdSender);
                rBuf.get(fourthSender);

                final Random r2 = new Random(SEED);

                for (int i = 0; i < BYTE_COUNT; i++) {
                    final byte expected = (byte) r2.nextInt();
                    assertEquals(expected, firstSender[i]);
                    assertEquals(expected, secondSender[i]);
                    assertEquals(expected, thirdSender[i]);
                    assertEquals(expected, fourthSender[i]);
                }
            }
        });
    }

    /**
     * This test is here to check that Object serialization and deserialization
     */
    @SuppressWarnings("deprecation")
    @Test(timeout = 10000)
    public void objectTransfer() throws MPIException {
        // Create a DummyObject
        final DummyObject a = new DummyObject(42);

        // Serialize the Dummy Object
        final ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();
        final ObjectOutput s = new ObjectOutput(byteOutStream);
        s.writeObject(a);
        s.close();
        final int size = byteOutStream.size();

        finish(() -> {
            for (final Place p : WORLD.places()) {
                if (p.equals(ROOT)) {
                    continue;
                }
                asyncAt(p, () -> {
                    // Receive the bytes broadcast by the ROOT
                    final int toReceive = WORLD.bCast1(0, ROOT);
                    final ByteBuffer receptionBuffer = MPI.newByteBuffer(size);

                    WORLD.comm.bcast(receptionBuffer, toReceive, MPI.BYTE, WORLD.rank(ROOT));

                    final byte[] arrayFromPlace = new byte[size];
                    receptionBuffer.get(arrayFromPlace);
                    final ByteArrayInputStream in = new ByteArrayInputStream(arrayFromPlace);
                    final ObjectInput ds = new ObjectInput(in);

                    final Object o = ds.readObject();

                    assertEquals(DummyObject.class.getCanonicalName(), o.getClass().getCanonicalName());
                    final DummyObject dummy = (DummyObject) o;
                    assertEquals(42, dummy.val);
                });
            }

            WORLD.bCast1(size, ROOT);
            final byte[] toSend = byteOutStream.toByteArray();
            assertEquals(size, toSend.length);

            WORLD.comm.bcast(toSend, size, MPI.BYTE, WORLD.rank(ROOT));

        });
    }

    @Before
    public void setupBefore() {
        WORLD = TeamedPlaceGroup.getWorld();
        ROOT = here();
    }
}
