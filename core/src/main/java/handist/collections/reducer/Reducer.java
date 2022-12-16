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
package handist.collections.reducer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.concurrent.Semaphore;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.dist.util.BufferFactory;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import mpi.MPI;
import mpi.MPIException;

/**
 * Reducer object. This object provides an abstract reduction operation on some
 * data element T. Implementations should provide the {@link #reduce(Object)},
 * {@link #merge(Reducer)} and {@link #newReducer()} methods.
 * <p>
 * As an example implementation, we provide the example below which performs the
 * sum over {@link Integer}s.
 *
 * <pre>
 * class SumReduction extends Reducer&lt;SumReduction, Integer&gt; {
 *     int runningSum = 0;
 *
 *     public void merge(SumReduction r) {
 *         runningSum += r.runningSum;
 *     }
 *
 *     public void reduce(Integer i) {
 *         runningSum += i;
 *     }
 *
 *     public void newReducer() {
 *         return new SumReduction();
 *     }
 * }
 * </pre>
 *
 * @author Patrick Finnerty
 *
 * @param <R> the implementing type itself, this is used to correctly implement
 *            reflective methods (methods that act on the implementing type
 *            itself)
 * @param <T> the type from which data is acquired to compute the reduction
 */
public abstract class Reducer<R extends Reducer<R, T>, T> implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 956660189595987110L;

    transient Semaphore semaphore = new Semaphore(0);
    transient int contribCounter = 0;
    transient Reducer<R, T> resultTeamedReduction = null;
    GlobalID reducerId = null;

    /**
     * Performs the global reduction of this instance. This method needs to be
     * called when the reduction operation took place over data distributed across
     * multiple hosts. In such a case, the reducer will have one instance per host
     * in which the local result if contained. This method is used to merge the
     * results of all the local reducers and make them reflect the global result of
     * the reduction.
     * <p>
     * An implementation does not need to override this method. However, users
     * should make sure that method {@link #merge(Reducer)} was appropriately
     * implemented. Otherwise, problems may arise.
     *
     * @param placeGroup the group of places on which "local" reductions have been
     *                   performed
     * @param gid        global id under which each local instance is registered
     * @throws IllegalStateException if this instance is not allowed to perform
     *                               global reductions
     */
    /*
     * This method needs improvements. More likely class Reducer needs some
     * modifications to handle global reduction operations that are not performed
     * under the GLB. Currently, the registering of a local Reducer instance needs
     * to be performed manually by the programmer, which does not satisfactorily
     * provides the global reduction features.
     */
    @SuppressWarnings({ "deprecation", "unchecked" })
    public void globalReduction(TeamedPlaceGroup placeGroup, GlobalID gid) {
        if (placeGroup == null || gid == null) {
            throw new IllegalArgumentException("Method Reducer#globalReduction does not tolerate null parameters");
        }

        final int reductionRank = placeGroup.rank();

        placeGroup.broadcastFlat(() -> {
            // Retrieve the local object through the global id
            final Reducer<R, T> local = (Reducer<R, T>) gid.getHere();

            final boolean isRoot = reductionRank == placeGroup.rank();
            final Place rootPlace = placeGroup.get(reductionRank);

            // 1. Serialize my local reducer
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            final ObjectOutput output = new ObjectOutput(out);
            output.writeObject(local);
            output.close();
            final byte[] localBuffer = out.toByteArray();

            // 2. Make a gather to tell the root how many bytes it will receive from
            // everyone
            final int nbBytesToSend = localBuffer.length;
            final int[] rcvSize = placeGroup.gather1(nbBytesToSend, rootPlace);

            if (isRoot) {
                // Compute the offsets and the total number of bites sent
                final int[] rcvOffset = new int[placeGroup.size()];
                int totalBytes = 0;
                for (int i = 0; i < rcvSize.length; i++) {
                    rcvOffset[i] = totalBytes; // Set the receiver offsets
                    totalBytes += rcvSize[i]; // Count the total number of bytes which this place is going to receive
                }
                final ByteBuffer receiverbuffer = BufferFactory.getByteBuffer(totalBytes); // MPI.newByteBuffer(totalBytes);

                // Receive the bytes from the other hosts
                placeGroup.comm.gatherv(localBuffer, nbBytesToSend, MPI.BYTE, receiverbuffer, rcvSize, rcvOffset,
                        MPI.BYTE, reductionRank);

                // 3. Proceed to deserialize the object from each place
                final Reducer<R, T>[] reducers = new Reducer[placeGroup.size()];

                for (int i = 0; i < placeGroup.size(); i++) {
                    final byte[] receivedBytes = new byte[rcvSize[i]];
                    receiverbuffer.get(receivedBytes);
                    final ByteArrayInputStream inStream = new ByteArrayInputStream(receivedBytes);
                    final ObjectInput inObject = new ObjectInput(inStream);

                    reducers[i] = (Reducer<R, T>) inObject.readObject();
                    inObject.close();
                }
                BufferFactory.returnByteBuffer(receiverbuffer);

                // 4. Apply the merge method so that everything is computed
                for (int i = 1; i < placeGroup.size(); i++) {
                    if (i != reductionRank) {
                        local.merge((R) reducers[i]);
                    }
                }
            } else {
                // Send the bytes to the root of the reduction
                placeGroup.comm.gatherv(localBuffer, nbBytesToSend, MPI.BYTE, reductionRank);
            }
        });

    }

    /**
     * Merges the partial reduction given as parameter into this instance. This
     * method is used by the library when the reduction is being parallelized and
     * multiple instances of the implementing type obtained through method
     * {@link #newReducer()} have been used and the partial result of each of these
     * instances need to be combined into a single instance.
     *
     * @param reducer another instance of the implementing class whose partial
     *                reduction needs to be merged into this instance
     */
    public abstract void merge(R reducer);

    /**
     * Creates a new R instance and returns it. The returned element should be the
     * neutral element of the reduction operation being implemented, that is,
     * calling method {@link #merge(Reducer)} with the object returned by this
     * method without there being any call to {@link #reduce(Object)} should not
     * influence the result of the reduction, or throw any exception.
     *
     * @return a new R instance which is going to be used to parallelize the
     *         reduction
     */
    public abstract R newReducer();

    /**
     * Takes the object given as parameter and performs the reduction operation this
     * object implements.
     *
     * @param input an instance of the object on which the reduction operation is
     *              taking place
     */
    public abstract void reduce(T input);

    /**
     * This method performs a teamed reduction in which a binaryreduction tree is
     * manually implemented, followed by a broadcast of the resulting object to all
     * members of the communicator.
     *
     * @param placeGroup group within which the reduction is performed
     */
    @SuppressWarnings({ "deprecation", "unchecked" })
    public R teamReduction(TeamedPlaceGroup placeGroup) {
        if (placeGroup == null) {
            throw new IllegalStateException("This Reducer is not allowed to perform any teamed reduction");
        }

        final int myRank = placeGroup.rank();
        final int groupSize = placeGroup.size();
        final Place root = placeGroup.get(0);

        // With OpenMPI Java bindings, the Object datatype disappeared.
        // As a result, we need to:
        // 1. Serialize our object
        // 2. Allgather the bytes of each process's reducer instance
        // 3. Deserialize the objects
        // 4. Apply the merge function on the reducer instances coming from each host to

        // 1. Serialization
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutput output = new ObjectOutput(out); // TODO do we need to set boolean argument to true?
        output.writeObject(this);
        output.close();

        // 2. Make an allgather with the obtained bytes
        final byte[] localBuffer = out.toByteArray();

        // Obtain the number of bytes sent by each host
        final int[] rcvSize = placeGroup.allGather1(localBuffer.length);

        // Compute the offsets and the total number of bites sent
        final int[] rcvOffset = new int[placeGroup.size()];
        int totalBytes = 0;
        for (int i = 0; i < rcvSize.length; i++) {
            rcvOffset[i] = totalBytes; // Set the receiver offsets
            totalBytes += rcvSize[i]; // Count the total number of bytes which this place is going to receive
        }
        final ByteBuffer receiverbuffer = BufferFactory.getByteBuffer(totalBytes); // MPI.newByteBuffer(totalBytes);

        // Transfer all the bytes to all the hosts
        try {
            placeGroup.comm.allGatherv(localBuffer, localBuffer.length, MPI.BYTE, receiverbuffer, rcvSize, rcvOffset,
                    MPI.BYTE);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }

        // 3. Proceed to deserialize the object from each place
        final Reducer<R, T>[] reducers = new Reducer[placeGroup.size()];

        for (int i = 0; i < placeGroup.size(); i++) {
            final byte[] bytesFromProcess = new byte[rcvSize[i]];
            receiverbuffer.get(bytesFromProcess);
            final ByteArrayInputStream inStream = new ByteArrayInputStream(bytesFromProcess);
            final ObjectInput inObject = new ObjectInput(inStream);

            reducers[i] = (Reducer<R, T>) inObject.readObject();
            inObject.close();
        }
        BufferFactory.returnByteBuffer(receiverbuffer);
        // 4. Apply the merge method so that everything is computed
        for (int i = 1; i < placeGroup.size(); i++) {
            reducers[0].merge((R) reducers[i]);
        }

        return (R) reducers[0];

    }
}
