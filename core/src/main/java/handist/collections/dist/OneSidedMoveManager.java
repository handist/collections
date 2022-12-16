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

import static apgas.Constructs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import handist.collections.dist.util.BufferFactory;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;
import mpi.MPI;

/**
 * Implementation of {@link MoveManager} which provides features to make data
 * transfer at the sole initiative of the sender.
 *
 * @author Patrick Finnerty
 *
 */
public class OneSidedMoveManager implements MoveManager {

    /** Private counter used to generate tags for MPI calls */
    private static int intTag = 10;

    private static final int MINIMUM_TAG_VALUE = 10;

    /**
     * Private method used to assign tags for MPI calls.
     *
     * @return a safe integer to use as tag
     */
    /*
     * When refactoring of MPI features access is made, this method should be moved.
     */
    protected static synchronized int nextTag() {
        final int toReturn = intTag++;
        if (intTag < 0) {
            intTag = MINIMUM_TAG_VALUE;
        }
        return toReturn;
    }

    /**
     * List of deserializers used to process the received data on the destination
     */
    final protected List<DeSerializer> deserializers;
    /**
     * Place to which the objects need to be sent
     */
    final protected Place destination;

    /**
     * List of serializers used to transform the objects of the local host into
     * bytes to be sent to the remote host
     */
    final protected List<Serializer> serializers;

    /**
     * Constructor
     *
     * @param d the place which will be the destination of all the objects
     *          transferred
     */
    public OneSidedMoveManager(Place d) {
        destination = d;
        deserializers = new ArrayList<>();
        serializers = new ArrayList<>();
    }

    /**
     * Sends the elements that have been requested to this move manager. The
     * elements will be received on the remote host with an asynchronous task
     * registered within the same Finish instance as the calling thread.
     *
     * @throws IOException if thrown during the serialization process
     */
    @SuppressWarnings("deprecation")
    public void asyncSend() throws IOException {
        final byte[] bytesToSend = prepareByteArray();

        final int nbOfBytes = bytesToSend.length;
        final int destinationRank = TeamedPlaceGroup.world.rank(destination);
        final int myRank = TeamedPlaceGroup.world.rank();
        final int tag = nextTag();

        asyncAt(destination, () -> {
            // Receive the array of bytes
            final ByteBuffer buffer = BufferFactory.getByteBuffer(nbOfBytes); // MPI.newByteBuffer(nbOfBytes);
            TeamedPlaceGroup.world.comm.recv(buffer, nbOfBytes, MPI.BYTE, myRank, tag);
            final byte[] bytesReceived = new byte[nbOfBytes];
            buffer.get(bytesReceived);
            final ByteArrayInputStream inStream = new ByteArrayInputStream(bytesReceived);
            final ObjectInput oInput = new ObjectInput(inStream);

            // The first object to come out of the byte array is a list of deserializers
            @SuppressWarnings("unchecked")
            final List<DeSerializer> ds = (List<DeSerializer>) oInput.readObject();

            // We know apply each deserializer one after the other
            for (final DeSerializer deserializer : ds) {
                deserializer.accept(oInput);
            }
            oInput.close();
            BufferFactory.returnByteBuffer(buffer);
        });

        async(() -> TeamedPlaceGroup.world.comm.send(bytesToSend, nbOfBytes, MPI.BYTE, destinationRank, tag));
    }

    /**
     * Applies all the serializers accumulated so far and produces a byte array
     * which is going to be transmitted to the destination
     *
     * @return a byte array containing the deserializers and the serialized form of
     *         the objects that were targeted by the serializers
     * @throws IOException if thrown during serialization of objects
     */
    protected byte[] prepareByteArray() throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final ObjectOutput oo = new ObjectOutput(stream);

        oo.writeObject(deserializers); // Write all the serializers first
        for (final Serializer s : serializers) {
            s.accept(oo); // Convert the objects targeted by the serializers into bytes.
        }
        oo.close();

        final byte[] bytesToSend = stream.toByteArray();
        return bytesToSend;
    }

    @Override
    public void request(Place dest, Serializer s, DeSerializer d) {
        if (dest.id != destination.id) {
            throw new RuntimeException("OneSidedMoveManager received a request for " + dest
                    + " but is only accepting submissions for" + destination);
        }
        serializers.add(s);
        deserializers.add(d);
    }

    /**
     * Sends the elements that have been requested in a synchronous fashion. This
     * method will only return when the remote host has completed the reception of
     * all the objects and has processed all of them with their deserializers
     *
     * @throws IOException if thrown during the serialization process
     */
    @SuppressWarnings("deprecation")
    public void send() throws IOException {
        final byte[] bytesToSend = prepareByteArray();

        final int nbOfBytes = bytesToSend.length;
        final int destinationRank = TeamedPlaceGroup.world.rank(destination);
        final int myRank = TeamedPlaceGroup.world.rank();
        final int tag = nextTag();

        async(() -> TeamedPlaceGroup.world.comm.send(bytesToSend, nbOfBytes, MPI.BYTE, destinationRank, tag));

        at(destination, () -> {
            // Receive the array of bytes
            final ByteBuffer buffer = BufferFactory.getByteBuffer(nbOfBytes); // MPI.newByteBuffer(nbOfBytes);
            TeamedPlaceGroup.world.comm.recv(buffer, nbOfBytes, MPI.BYTE, myRank, tag);
            final byte[] bytesReceived = new byte[nbOfBytes];
            buffer.get(bytesReceived);
            final ByteArrayInputStream inStream = new ByteArrayInputStream(bytesReceived);
            final ObjectInput oInput = new ObjectInput(inStream);

            // The first object to come out of the byte array is a list of deserializers
            @SuppressWarnings("unchecked")
            final List<DeSerializer> ds = (List<DeSerializer>) oInput.readObject();

            // We know apply each deserializer one after the other
            for (final DeSerializer deserializer : ds) {
                deserializer.accept(oInput);
            }
            BufferFactory.returnByteBuffer(buffer);
            oInput.close();
        });

    }
}
