package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import apgas.Place;
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

        TeamedPlaceGroup.world.comm.Isend(bytesToSend, 0, nbOfBytes, MPI.BYTE, destinationRank, tag);

        asyncAt(destination, () -> {
            // Receive the array of bytes
            TeamedPlaceGroup.world.comm.Recv(new byte[nbOfBytes], 0, nbOfBytes, MPI.BYTE, myRank, tag);
            final ByteArrayInputStream inStream = new ByteArrayInputStream(bytesToSend);
            final ObjectInput oInput = new ObjectInput(inStream);

            // The first object to come out of the byte array is a list of deserializers
            @SuppressWarnings("unchecked")
            final List<DeSerializer> ds = (List<DeSerializer>) oInput.readObject();

            // We know apply each deserializer one after the other
            for (final DeSerializer deserializer : ds) {
                deserializer.accept(oInput);
            }
            oInput.close();
        });
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
        if (dest != destination) {
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

        TeamedPlaceGroup.world.comm.Isend(bytesToSend, 0, nbOfBytes, MPI.BYTE, destinationRank, tag);

        at(destination, () -> {
            // Receive the array of bytes
            TeamedPlaceGroup.world.comm.Recv(new byte[nbOfBytes], 0, nbOfBytes, MPI.BYTE, myRank, tag);
            final ByteArrayInputStream inStream = new ByteArrayInputStream(bytesToSend);
            final ObjectInput oInput = new ObjectInput(inStream);

            // The first object to come out of the byte array is a list of deserializers
            @SuppressWarnings("unchecked")
            final List<DeSerializer> ds = (List<DeSerializer>) oInput.readObject();

            // We know apply each deserializer one after the other
            for (final DeSerializer deserializer : ds) {
                deserializer.accept(oInput);
            }
            oInput.close();
        });
    }
}
