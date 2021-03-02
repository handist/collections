package handist.collections.glb;

import static apgas.ExtendedConstructs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import apgas.Place;
import apgas.SerializableJob;
import apgas.impl.Finish;
import handist.collections.dist.MoveManager;
import handist.collections.dist.OneSidedMoveManager;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;
import mpi.MPI;

/**
 * Extension of class {@link OneSidedMoveManager} with more advanced and
 * specific functionalities used in the context of the GLB. The functionalities
 * of this class would be otherwise confusing to programmers. There are
 * therefore presented with package visibility so as to avoid exposing them
 * outside of this package.
 *
 * @author Patrick Finnerty
 *
 */
class CustomOneSidedMoveManager extends OneSidedMoveManager {

    /**
     * Constructor
     *
     * @param d place to which instances will be transferred
     */
    public CustomOneSidedMoveManager(Place d) {
        super(d);
    }

    /**
     * Performs the transfer of instances using an asynchronous task. The
     * serializers that were registered into this MoveManager are applied to
     * generate a byte array. The deserializers are serialized using the default
     * serialization method used by the APGAS library, most likely "kryo". After the
     * deserialization has completed, calls the job passed as parameter, making it
     * registered with the same finish instances that were passed as parameter.
     *
     * @param j       the job to run after the transfer of instances has completed
     * @param finishs {@link Finish} instances under which the asynchronous task in
     *                charge of making the transfer and the job given as parameter
     *                will be registered.
     * @throws IOException if thrown during the serialization of instances
     */
    public void asyncSendAndDoNoMPI(SerializableJob j, Finish... finishs) throws IOException {
        final byte[] bytesToSend = serializeObjectsOnly();

        final List<DeSerializer> ds = deserializers;

        asyncArbitraryFinish(destination, () -> {
            // Convert the array of bytes implicitly serialized into a byte stream
            final ByteArrayInputStream inStream = new ByteArrayInputStream(bytesToSend);
            final ObjectInput oInput = new ObjectInput(inStream);

            // Apply the deserializers
            for (final DeSerializer d : ds) {
                d.accept(oInput);
            }
            oInput.close();

            // Call the job that was passed as parameter
            j.run();
        }, finishs);
    }

    /**
     * Proceed to the serialization and send the bytes over to the destination. Also
     * spawn an asynchronous task on the remote place to receive the bytes and
     * deserialize them.
     *
     * @param j       the job to run after the deserialization of the objects that
     *                were transferred
     * @param finishs the finishes under which the asynchronous task which is
     *                spawned on the destination host is registered.
     * @throws IOException if thrown during serialization
     */
    @SuppressWarnings("deprecation")
    public void asyncSendAndDoWithMPI(SerializableJob j, Finish... finishs) throws IOException {
        final byte[] bytesToSend = prepareByteArray();

        final int nbOfBytes = bytesToSend.length;
        final int destinationRank = TeamedPlaceGroup.getWorld().rank(destination);
        final int myRank = TeamedPlaceGroup.getWorld().rank();
        final int tag = nextTag();

        TeamedPlaceGroup.getWorld().comm.Isend(bytesToSend, 0, nbOfBytes, MPI.BYTE, destinationRank, tag);

        asyncArbitraryFinish(destination, () -> {
            // Receive the array of bytes
            TeamedPlaceGroup.getWorld().comm.Recv(new byte[nbOfBytes], 0, nbOfBytes, MPI.BYTE, myRank, tag);
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

            // Reception is over, launch the job that was given as parameter
            j.run();
        }, finishs);
    }

    /**
     * Returns the list of the deserializers that have been registered into this
     * move manager
     *
     * @return the list of deserializers registered into this {@link MoveManager}
     */
    public List<DeSerializer> getDeSerializer() {
        return deserializers;
    }

    /**
     * Similar to {@link OneSidedMoveManager} method used to prepare bytes, but does
     * not serialize the deserializers at the beginning of the array. Instead, only
     * the serializers are called and their output written to the returned array.
     *
     * @return byte array containing the serialized form of the objects targeted by
     *         the serializers
     * @throws IOException if thrown during the serialization process
     */
    public byte[] serializeObjectsOnly() throws IOException {
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final ObjectOutput oo = new ObjectOutput(stream);

        for (final Serializer s : serializers) {
            s.accept(oo); // Convert the objects targeted by the serializers into bytes.
        }
        oo.close();

        final byte[] bytesToSend = stream.toByteArray();
        return bytesToSend;
    }
}
