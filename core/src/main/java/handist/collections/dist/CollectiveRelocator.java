/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections.dist;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import apgas.Constructs;
import apgas.Place;
import handist.collections.dist.util.BufferFactory;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.Serializer;
import mpi.MPI;
import mpi.MPIException;

/**
 * Class wrapping utilities used to relocate object instances that rely on pairs
 * of serializers and deserializers and MPI functions.
 *
 * @author Patrick Finnerty
 *
 */
@SuppressWarnings("deprecation")
public class CollectiveRelocator {
    public static class Allgather {
        TeamedPlaceGroup pg;
        final List<Serializer> sers = new LinkedList<>();
        final List<DeSerializerUsingPlace> desers = new LinkedList<>();

        public Allgather(TeamedPlaceGroup pg) {
            this.pg = pg;
        }

        void execute() {
            final int numPlaces = pg.size();
            final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
            final ObjectOutput out = new ObjectOutput(out0);
            try {
                for (final Serializer ser : sers) {
                    ser.accept(out);
                }
            } catch (final IOException e) {
                System.err.println("IOException in CollectiveRelocator.allgather");
                e.printStackTrace(System.err);
                return;
            } finally {
                out.close();
            }
            final byte[] buf = out0.toByteArray();
            final int size = buf.length;

            // Prepare the reception buffer / index reception
            final int[] recvCounts = pg.allGather1(size);
            final int[] recvDispls = new int[numPlaces];
            int total = 0;
            for (int i = 0; i < recvCounts.length; i++) {
                recvDispls[i] = total;
                total += recvCounts[i];
            }
            final ByteBuffer rbuf = BufferFactory.getByteBuffer(total); // MPI.newByteBuffer(total);
            try {
                pg.comm.allGatherv(buf, size, MPI.BYTE, rbuf, recvCounts, recvDispls, MPI.BYTE);
            } catch (final MPIException e) {
                e.printStackTrace();
                throw new Error("[CollectiveRelocator] MPIException");
            }

            for (int i = 0; i < pg.size(); i++) {
                final Place provenance = pg.get(i);
                final byte[] bytesFromProcess = new byte[recvCounts[i]];
                rbuf.get(bytesFromProcess);
                if (Constructs.here().equals(provenance)) {
                    continue;
                }
                final ByteArrayInputStream in0 = new ByteArrayInputStream(bytesFromProcess);
                final ObjectInput in = new ObjectInput(in0);
                try {
                    for (final DeSerializerUsingPlace deser : desers) {
                        deser.accept(in, provenance);
                    }
                } catch (final Exception e) {
                    e.printStackTrace();
                    throw new Error("[CollectiveRelocator] DeSerialize error handled.");
                } finally {
                    in.close();
                }
            }
            BufferFactory.returnByteBuffer(rbuf);
        }

        Allgather request(Serializer ser, DeSerializerUsingPlace deser) {
            sers.add(ser);
            desers.add(deser);
            return this;
        }
    }

    public static class Bcast {
        TeamedPlaceGroup pg;
        Place root;
        List<Serializer> sers = new LinkedList<>();
        List<DeSerializer> desers = new LinkedList<>();

        public Bcast(TeamedPlaceGroup pg, Place root) {
            this.pg = pg;
            this.root = root;
        }

        void execute() {
            int size;
            if (Constructs.here().equals(root)) {
                final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
                final ObjectOutput out = new ObjectOutput(out0);
                try {
                    for (final Serializer ser : sers) {
                        ser.accept(out);
                    }
                } catch (final IOException e) {
                    e.printStackTrace();
                    throw new Error("[CollectiveRelocator] Serialize error raised.");
                } finally {
                    out.close();
                }

                // Inform other processes of the number of incoming bytes
                size = out0.size();
                pg.bCast1(size, root);

                // Broadcast the bytes
                try {
                    pg.comm.bcast(out0.toByteArray(), out0.size(), MPI.BYTE, pg.rank(root));
                } catch (final MPIException e) {
                    throw new RuntimeException(e);
                }
            } else {
                // Get the number of bytes to receive
                size = pg.bCast1(0, root);

                // Prepare a buffer of the appropriate size
                final ByteBuffer buf = BufferFactory.getByteBuffer(size); // MPI.newByteBuffer(size);
                try {
                    pg.comm.bcast(buf, size, MPI.BYTE, pg.rank(root));
                } catch (final MPIException e) {
                    throw new RuntimeException(e);
                }

                // Deserialize the received bytes
                final byte[] byteArray = new byte[size];
                buf.get(byteArray);
                final ObjectInput in = new ObjectInput(new ByteArrayInputStream(byteArray));
                try {
                    for (final DeSerializer des : desers) {
                        des.accept(in);
                    }
                } catch (final Exception e) {
                    e.printStackTrace();
                    throw new Error("[CollectiveRelocator] DeSerialize error raised.");
                } finally {
                    in.close();
                }
                BufferFactory.returnByteBuffer(buf);
            }
        }

        Bcast request(Serializer ser, DeSerializer des) {
            sers.add(ser);
            desers.add(des);
            return this;
        }
    }

    public static class Gather {
        TeamedPlaceGroup pg;
        Place root;
        List<Serializer> sers = new LinkedList<>();
        List<DeSerializerUsingPlace> desers = new LinkedList<>();

        public Gather(TeamedPlaceGroup pg, Place root) {
            this.pg = pg;
            this.root = root;
        }

        void execute() {
            final int numPlaces = pg.size();

            // Preparing the sub-index array to set the index at which bytes are received
            int[] recvDispls = null;

            if (Constructs.here().equals(root)) {
                final int[] recvCounts = pg.gather1(0, root);
                recvDispls = new int[numPlaces];
                int total = 0;
                for (int i = 0; i < recvCounts.length; i++) {
                    recvDispls[i] = total;
                    total += recvCounts[i];
                }
                // Receive bytes
                final ByteBuffer rbuf = BufferFactory.getByteBuffer(total); // MPI.newByteBuffer(total);
                try {
                    pg.comm.gatherv(rbuf, recvCounts, recvDispls, MPI.BYTE, pg.rank(root));
                } catch (final MPIException e1) {
                    e1.printStackTrace();
                    throw new Error("[CollectiveRelocator] DeSerialize error raised.");
                }

                // If on root, proceed to deserialize the received bytes
                for (int i = 0; i < recvCounts.length; i++) {
                    final Place sender = pg.get(i);
                    if (sender.equals(root)) {
                        continue;
                    }
                    // Initialize array for bytes received
                    final byte[] bytesFromPlace = new byte[recvCounts[i]];
                    // Obtain the bytes from the buffer
                    rbuf.get(bytesFromPlace);

                    // Re-create all the object from the received bytes
                    final ByteArrayInputStream in0 = new ByteArrayInputStream(bytesFromPlace);
                    final ObjectInput in = new ObjectInput(in0);
                    try {
                        for (final DeSerializerUsingPlace deser : desers) {
                            deser.accept(in, sender);
                        }
                    } catch (final Exception e) {
                        e.printStackTrace();
                        throw new Error("[CollectiveRelocator] DeSerialize error raised.");
                    } finally {
                        in.close();
                    }
                }
                BufferFactory.returnByteBuffer(rbuf);
            } else {
                final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
                final ObjectOutput out = new ObjectOutput(out0);
                try {
                    for (final Serializer ser : sers) {
                        ser.accept(out);
                    }
                } catch (final IOException exp) {
                    throw new Error("This should not occur!.");
                } finally {
                    out.close();
                }
                final byte[] buf = out0.toByteArray();

                // Exchanging the number of bytes to receive
                final int size = buf.length;
                pg.gather1(size, root);
                try {
                    pg.comm.gatherv(buf, size, MPI.BYTE, pg.rank(root));
                } catch (final MPIException e) {
                    e.printStackTrace();
                    throw new Error("[CollectiveRelocator] DeSerialize error raised.");
                }
            }
        }

        Gather request(Serializer ser, DeSerializerUsingPlace deser) {
            sers.add(ser);
            desers.add(deser);
            return this;
        }
    }

    private static final boolean DEBUG = false;

    /**
     * Transfers some bytes from and to all the places in the place group, returning
     * a byte array containing all the bytes sent by the other places in the group
     * to this place.
     * <p>
     * This method is actually implemented with 2 successive MPI calls. The first
     * one is used to exchange information about the number of bytes each place want
     * to transmit to every other place. With this information, each place prepares
     * a receiver array of the appropriate size. The second MPI call is when the
     * actual byte transfer occurs.
     *
     * @param placeGroup group of places participating in the exchange
     * @param byteArray  array containing the bytes that this place want to send
     * @param sendOffset offsets indicating the starting position in the array for
     *                   the bytes that need to be transferred to every other place
     *                   in the group
     * @param sendSize   number of bytes in the array to be sent to every host
     * @param rcvOffset  array in which the offsets indicating where the bytes
     *                   received from every place start will be placed. This
     *                   parameter needs to be an array initialized with a size that
     *                   matches the number of places in this group.
     * @param rcvSize    number of bytes received from each host in the group. This
     *                   parameter needs to be an array initialized with a size that
     *                   matches the number of places in this group.
     * @return an array containing the bytes received from every place
     * @throws MPIException
     */
    static ByteBuffer exchangeBytesWithinGroup(TeamedPlaceGroup placeGroup, byte[] byteArray, int[] sendOffset,
            int[] sendSize, int[] rcvOffset, int[] rcvSize) throws MPIException {
        placeGroup.comm.allToAll(sendSize, 1, MPI.INT, rcvSize, 1, MPI.INT);
        if (DEBUG) {
            final StringBuffer buf = new StringBuffer();
            buf.append(Constructs.here() + "::");
            for (int j = 0; j < rcvSize.length; j++) {
                buf.append(":" + rcvSize[j]);
            }
            System.out.println(buf.toString());
        }

        int current = 0;
        for (int i = 0; i < rcvSize.length; i++) {
            rcvOffset[i] = current; // Set the receiver offsets
            current += rcvSize[i]; // Count the total number of bytes which this place is going to receive
        }

        // Initialize a reception array of the adequate size
        final ByteBuffer recvbuf = BufferFactory.getByteBuffer(current);// MPI.newByteBuffer(current);

        // Do the transfer
        placeGroup.comm.allToAllv(byteArray, sendSize, sendOffset, MPI.BYTE, recvbuf, rcvSize, rcvOffset, MPI.BYTE);

        BufferFactory.returnByteBuffer(recvbuf);
        // Return the initialized receiver array which now contains the received bytes.
        return recvbuf;
    }
}
