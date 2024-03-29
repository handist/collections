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

import apgas.Constructs;
import apgas.Place;
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
class CollectiveRelocator {

    private static final boolean DEBUG = false;

    static void allgatherSer(TeamedPlaceGroup pg, Serializer ser, DeSerializerUsingPlace deser) {
        final int numPlaces = pg.size();
        final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutput(out0);
        try {
            ser.accept(out);
        } catch (final IOException exp) {
            throw new Error("This should not occur!.");
        } finally {
            out.close();
        }
        final byte[] buf = out0.toByteArray();
        final int size = buf.length;
        final int[] tmpCounts = new int[1];
        tmpCounts[0] = size;
        final int[] recvCounts = new int[numPlaces];
        final int[] recvDispls = new int[numPlaces];
        try {
            pg.comm.Allgather(tmpCounts, 0, 1, MPI.INT, recvCounts, 0, 1, MPI.INT);
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("[CollectiveRelocator] MPIException");
        }

        int total = 0;
        for (int i = 0; i < recvCounts.length; i++) {
            recvDispls[i] = total;
            total += recvCounts[i];
        }
        final byte[] rbuf = new byte[total];
        try {
            pg.comm.Allgatherv(buf, 0, size, MPI.BYTE, rbuf, 0, recvCounts, recvDispls, MPI.BYTE);
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("[CollectiveRelocator] MPIException");
        }

        for (int i = 0; i < recvCounts.length; i++) {
            if (Constructs.here().equals(pg.get(i))) {
                continue;
            }
            final ByteArrayInputStream in0 = new ByteArrayInputStream(rbuf, recvDispls[i], recvCounts[i]);
            final ObjectInput in = new ObjectInput(in0);
            try {
                deser.accept(in, pg.get(i));
            } catch (final Exception e) {
                e.printStackTrace();
                throw new Error("[CollectiveRelocator] DeSerialize error handled.");
            } finally {
                in.close();
            }
        }
    }

    static void bcastSer(TeamedPlaceGroup pg, Place root, Serializer ser, DeSerializer des) throws MPIException {
        final int[] tmpBuf = new int[1];
        if (Constructs.here().equals(root)) {
            final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
            final ObjectOutput out = new ObjectOutput(out0);
            try {
                ser.accept(out);
            } catch (final IOException e) {
                e.printStackTrace();
                throw new Error("[CollectiveRelocator] Serialize error raised.");
            } finally {
                out.close();
            }
            tmpBuf[0] = out0.size();
            pg.comm.Bcast(tmpBuf, 0, 1, MPI.INT, pg.rank(root));
            pg.comm.Bcast(out0.toByteArray(), 0, out0.size(), MPI.BYTE, pg.rank(root));
        } else {
            pg.comm.Bcast(tmpBuf, 0, 1, MPI.INT, pg.rank(root));
            final byte[] buf = new byte[tmpBuf[0]];
            pg.comm.Bcast(buf, 0, buf.length, MPI.BYTE, pg.rank(root));
            final ObjectInput in = new ObjectInput(new ByteArrayInputStream(buf));
            try {
                des.accept(in);
            } catch (final Exception e) {
                e.printStackTrace();
                throw new Error("[CollectiveRelocator] DeSerialize error raised.");
            } finally {
                in.close();
            }
        }
    }

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
    static byte[] exchangeBytesWithinGroup(TeamedPlaceGroup placeGroup, byte[] byteArray, int[] sendOffset,
            int[] sendSize, int[] rcvOffset, int[] rcvSize) throws MPIException {
        placeGroup.comm.Alltoall(sendSize, 0, 1, MPI.INT, rcvSize, 0, 1, MPI.INT);
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
        final byte[] recvbuf = new byte[current];

        // Do the transfer
        placeGroup.Alltoallv(byteArray, 0, sendSize, sendOffset, MPI.BYTE, recvbuf, 0, rcvSize, rcvOffset, MPI.BYTE);

        // Return the initialized receiver array which now contains the received bytes.
        return recvbuf;
    }

    static void gatherSer(TeamedPlaceGroup pg, Place root, Serializer ser, DeSerializerUsingPlace deser) {
        final int numPlaces = pg.size();
        final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
        final ObjectOutput out = new ObjectOutput(out0);
        try {
            ser.accept(out);
        } catch (final IOException exp) {
            throw new Error("This should not occur!.");
        } finally {
            out.close();
        }
        final byte[] buf = out0.toByteArray();
        final int size = buf.length;
        final int[] tmpCounts = new int[1];
        tmpCounts[0] = size;
        final int[] recvCounts = new int[numPlaces];
        final int[] recvDispls = new int[numPlaces];
        try {
            pg.comm.Gather(tmpCounts, 0, 1, MPI.INT, recvCounts, 0, 1, MPI.INT, pg.rank(root));
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("[CollectiveRelocator] MPIException");
        }

        int total = 0;
        for (int i = 0; i < recvCounts.length; i++) {
            recvDispls[i] = total;
            total += recvCounts[i];
        }
        final byte[] rbuf = Constructs.here().equals(root) ? new byte[total] : null;
        try {
            pg.comm.Gatherv(buf, 0, size, MPI.BYTE, rbuf, 0, recvCounts, recvDispls, MPI.BYTE, pg.rank(root));
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("[CollectiveRelocator] MPIException");
        }

        if (!Constructs.here().equals(root)) {
            return;
        }
        for (int i = 0; i < recvCounts.length; i++) {
            if (Constructs.here().equals(pg.get(i))) {
                continue;
            }
            final ByteArrayInputStream in0 = new ByteArrayInputStream(rbuf, recvDispls[i], recvCounts[i]);
            final ObjectInput in = new ObjectInput(in0);
            try {
                deser.accept(in, pg.get(i));
            } catch (final Exception e) {
                e.printStackTrace();
                throw new Error("[CollectiveRelocator] DeSerialize error raised.");
            } finally {
                in.close();
            }
        }
    }
}
