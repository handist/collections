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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import apgas.Place;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

/**
 * This class is used for relocating elements of distributed collections in a
 * collective manner. I.e. the transfer of instances between places occurs when
 * an instance of this relocator's {@link #sync()} method is called on every
 * host involved in the transfer.
 */
public final class CollectiveMoveManager implements MoveManager {
    private static final boolean DEBUG = false;

    /**
     * Collection of the deserializers, gathered by destination place.
     */
    private final Map<Place, List<DeSerializer>> builders;

    /**
     * The group of places which are involved in the collective relocation
     * operation.
     */
    private final TeamedPlaceGroup placeGroup;

    /**
     * The collection of serializers, gathered by destination places
     */
    private final Map<Place, List<Serializer>> serializeListMap;

    private boolean references = true;

    /**
     * Construct a MoveManagerLocal with the given arguments.
     *
     * @param placeGroup the group hosts that will transfer objects between
     *                   themselves using this instance.
     */
    public CollectiveMoveManager(TeamedPlaceGroup placeGroup) {
        this.placeGroup = placeGroup;
        serializeListMap = new HashMap<>(placeGroup.size());
        builders = new HashMap<>(placeGroup.size());
        for (final Place place : placeGroup.places()) {
            serializeListMap.put(place, new ArrayList<>());
            builders.put(place, new ArrayList<>());
        }
    }

    private void all2allser() throws Exception {
        // Prepare to send the data
        final int[] sendOffset = new int[placeGroup.size()];
        final int[] sendSize = new int[placeGroup.size()];
        // Rather than initializing an array, use an output stream as we do not know how
        // long the array needs to be an advance.
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        executeSerialization(out, sendOffset, sendSize);

//        final Place here = here();
//        final int myRank = placeGroup.rank(here);
//        System.out.print(here + "-rank" + myRank + ":");
//        for (final int s : sendSize) {
//            System.out.print(s + " ");
//        }
//        System.out.println();

        // Prepare the arrays for receiving the information
        final int[] rcvOffset = new int[placeGroup.size()];
        final int[] rcvSize = new int[placeGroup.size()];

        // Make the MPI call
        final ByteBuffer buf = CollectiveRelocator.exchangeBytesWithinGroup(placeGroup, out.toByteArray(), sendOffset,
                sendSize, rcvOffset, rcvSize);

        // Deserialize the objects received from the various hosts
        executeDeserialization(buf, rcvOffset, rcvSize);
    }

    /**
     * Removes all the serializers and deserializers contained in this instance
     */
    public void clear() {
        for (final List<Serializer> list : serializeListMap.values()) {
            list.clear();
        }
        for (final List<DeSerializer> list : builders.values()) {
            list.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private void executeDeserialization(ByteBuffer buf, int[] rcvOffset, int[] rcvSize) throws Exception {
        for (int i = 0; i < placeGroup.size(); i++) {
            final Place p = placeGroup.get(i);
            if (p.equals(here())) {
                continue;
            }

            final int size = rcvSize[i];
            final byte[] arrayFromPlace = new byte[size];
            buf.get(arrayFromPlace);

            final ByteArrayInputStream in = new ByteArrayInputStream(arrayFromPlace);
            final ObjectInput ds = new ObjectInput(in, references);
            final List<DeSerializer> deserializerList = (List<DeSerializer>) ds.readObject();
            for (final DeSerializer deserialize : deserializerList) {
                deserialize.accept(ds);
            }
            ds.close();
        }
    }

    /*
     * 将来的に moveAtSync(dist:RangedDistribution, mm) を 持つものを interface 宣言するのかな？
     * public def moveAssociativeCollectionsAtSync(dist: RangedDistribution, dists:
     * List[RangedMoballe]) {
     *
     * } public def moveAssosicativeCollectionsAtSync(dist: Distribution[K]) { //
     * add dist to the list to schedule }
     */
    /**
     * Proceed to call all the serializers held by this instance and prepare an
     * array for an incoming MPI call.
     *
     * @param out     output stream into which the serialized objects need to be
     *                placed
     * @param offsets array describing the index in the byte array which is destined
     *                for every destination
     * @param sizes   length in the array which is destined to every host
     * @throws IOException if thrown while serializing the objects
     */
    private void executeSerialization(ByteArrayOutputStream out, int[] offsets, int[] sizes) throws IOException {
        for (int i = 0; i < placeGroup.size(); i++) {
            final Place place = placeGroup.get(i);
            offsets[i] = out.size();
            if (place.equals(here())) {
                continue;
            }

            // TODO should reopen ByteArray...
            if (DEBUG) {
                System.err.println("execSeri: " + here() + "->" + place + ":start:" + out.size());
            }
            final ObjectOutput s = new ObjectOutput(out, references);
            // First, write all the deserializers which will have to operate on the other
            // end
            s.writeObject(builders.get(place));
            // Then call all the serializers
            for (final Serializer serializer : serializeListMap.get(place)) {
                serializer.accept(s);
            }
            s.close();
            if (DEBUG) {
                System.err.println("execSeri: " + here() + "->" + place + ":finish:" + out.size());
            }
            sizes[i] = out.size() - offsets[i];
        }
    }

    @Override
    public void request(Place pl, Serializer serializer, DeSerializer deserializer) {
        serializeListMap.get(pl).add(serializer);
        builders.get(pl).add(deserializer);
    }

    /**
     * Request to reset the Serializer at the specified place.
     *
     * @param pl the target place.
     */
    public void reset(Place pl) {
        serializeListMap.get(pl).add((ObjectOutput s) -> {
            s.reset();
        });
    }

    /**
     *
     * */
    public void setReferences(boolean references) {
        this.references = references;
    }

    /**
     * Execute the all requests synchronously. When the transfer of objects
     * completes, clears this object so that it can be safely re-used for another
     * transfer.
     *
     * @throws Exception if a runtime exception is thrown at any stage during the
     *                   relocation
     */
    public void sync() throws Exception {
        all2allser();
        // Clear the MoveManager to make it safe to reuse
        clear();
    }
}
