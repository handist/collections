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

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.Bag;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.MemberOfLazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.Serializer;
import mpi.MPI;
import mpi.MPIException;

/**
 * A class for handling objects at multiple places. It is allowed to add new
 * elements dynamically. This class provides methods for load balancing.
 * <p>
 * Note: In its current implementation, there are some limitations.
 * <ul>
 * <li>There is only one load balancing method
 * <li>The method flattens the number of elements of the all places
 * </ul>
 *
 * @param <T> type of the elements handled by the {@link DistBag}.
 */
public class DistBag<T> extends Bag<T> implements DistributedCollection<T, DistBag<T>>, SerializableWithReplace {
    /* implements Container[T], ReceiverHolder[T] */

    /**
     * Implementation of the GLOBAL handle for class {@link DistBag}
     *
     * @author Patrick Finnerty
     *
     */
    public class DistBagGlobal extends GlobalOperations<T, DistBag<T>> {
        /**
         * Constructor
         *
         * @param handle handle to the local {@link DistBag} this GLOBAL handle acts on
         */
        DistBagGlobal(DistBag<T> handle) {
            super(handle);
        }

        @Override
        public Object writeReplace() throws ObjectStreamException {
            final TeamedPlaceGroup pg1 = placeGroup;
            final GlobalID gId = id;
            return new MemberOfLazyObjectReference<>(pg1, gId, () -> {
                return new DistBag<>(pg1, gId);
            }, (distBag) -> {
                return distBag.GLOBAL;
            });
        }

    }

    /**
     * Implementation of the TEAM handle for class {@link DistBag}
     *
     * @author Patrick Finnerty
     *
     */
    public class DistBagTeam extends TeamOperations<T, DistBag<T>> {

        /**
         * Constructor
         *
         * @param handle handle to the local {@link DistBag} this TEAM handle acts on
         */
        DistBagTeam(DistBag<T> handle) {
            super(handle);
        }

        @SuppressWarnings("deprecation")
        @Override
        public void size(long[] result) {
            final TeamedPlaceGroup pg = handle.placeGroup();
            result[pg.myrank] = handle.size();
            try {
                // THIS WORKS FOR MPJ-NATIVE implementation
                pg.comm.Allgather(result, pg.myrank, 1, MPI.LONG, result, 0, 1, MPI.LONG);
            } catch (final MPIException e) {
                e.printStackTrace();
                throw new Error("[DistMap] network error in team().size()");
            }
        }

        @Override
        public void updateDist() {
            // TODO Auto-generated method stub
        }

    }

    private static int _debug_level = 5;

    /** Handle to Global operations on the DistBag instance */
    public DistBag<T>.DistBagGlobal GLOBAL;
    /**
     * Global Id which identifies this DistBag object as part of a number of handles
     * to the distributed collection implemented by this instance
     */
    final GlobalID id;

    /**
     * Array keeping track of the number of entries on the various places on which
     * this distributed collection is defined.
     */
    public transient float[] locality;

    /**
     * Set of Places on which this {@link DistBag} is defined, i.e. on which Places
     * can this distributed collection can hold instances.
     */
    public final TeamedPlaceGroup placeGroup;

    /**
     * Handle to TEAM operations on this DistBag instance
     */
    public DistBag<T>.DistBagTeam TEAM;

    /**
     * Create a new DistBag. Place.places() is used as the PlaceGroup.
     */
    public DistBag() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Create a new DistBag using the given arguments.
     *
     * @param pg the places susceptible to interact with this instance.
     */
    public DistBag(TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }

    /**
     * Constructor for DistBag.
     *
     * @param pg       group of places on which this contruction is to be
     *                 initialized
     * @param globalId unique identifier linked to every local {@link DistBag}
     *                 instance which participates in this distributed collection
     */
    protected DistBag(TeamedPlaceGroup pg, GlobalID globalId) {
        super();
        id = globalId;
        placeGroup = pg;
        locality = new float[pg.size];
        Arrays.fill(locality, 1.0f);
        id.putHere(this);
        GLOBAL = new DistBagGlobal(this);
        TEAM = new DistBagTeam(this);
    }

    @Override
    public void forEach(SerializableConsumer<T> action) {
        super.forEach(action);
    }

    /**
     * gather all place-local elements to the root Place.
     *
     * @param root the place where the result of reduction is stored.
     */
    @SuppressWarnings("unchecked")
    public void gather(Place root) {
        final Serializer serProcess = (ObjectOutput s) -> {
            s.writeObject(new Bag<>(this));
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place place) -> {
            final Bag<T> imported = (Bag<T>) ds.readObject();
            addBag(imported);
        };
        CollectiveRelocator.gatherSer(placeGroup, root, serProcess, desProcess);
        if (!here().equals(root)) {
            clear();
        }
    }

    /**
     * Return a Container that has the same values of DistBag's local storage.
     *
     * @return a Container that has the same values of local storage.
     */
    /*
     * public Collection<T> clone(): Container[T] { return data.clone(); }
     */

    @Override
    public GlobalOperations<T, DistBag<T>> global() {
        return GLOBAL;
    }

    /*
     * public def integrate(src : List[T]) { // addAll(src); throw new
     * UnsupportedOperationException(); }
     */

    @Override
    public GlobalID id() {
        return id;
    }

    @Override
    public float[] locality() {
        return locality;
    }

    @Override
    public void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManager mm) throws Exception {
        for (final IntLongPair pair : moveList) {
            if (_debug_level > 5) {
                System.out.println("MOVE src: " + here() + " dest: " + pair.first + " size: " + pair.second);
            }
            if (pair.second > Integer.MAX_VALUE) {
                throw new Error("One place cannot receive so much elements: " + pair.second);
            }
            moveAtSyncCount((int) pair.second, placeGroup.get(pair.first), mm);
        }
    }

    /**
     * Removes the specified number of entries from the local Bag and prepares them
     * to be transfered to the specified place when the
     * {@link CollectiveMoveManager#sync()} method of the
     * {@link CollectiveMoveManager} is called.
     * <p>
     * The objects are not removed from the local collection until method
     * {@link CollectiveMoveManager#sync()} is called. If the {@code destination} is
     * the local placce, this method has no effects.
     *
     * @param count       number of objects to move from this instance
     * @param destination the destination of the objects
     * @param mm          move manager in charge of making the transfer
     */
    @SuppressWarnings("unchecked")
    public void moveAtSyncCount(final int count, Place destination, MoveManager mm) {
        if (destination.equals(Constructs.here())) {
            return;
        }
        final DistBag<T> collection = this;
        final Serializer serialize = (ObjectOutput s) -> {
            s.writeObject(this.remove(count));
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final List<T> imported = (List<T>) ds.readObject();
            collection.addBag(imported);
        };
        mm.request(destination, serialize, deserialize);
    }

    @Override
    public void parallelForEach(SerializableConsumer<T> action) {
        super.parallelForEach(action);
    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    @SuppressWarnings("unused")
    @Deprecated
    private void setupBranches(SerializableBiConsumer<Place, DistBag<T>> gen) {
        final DistBag<T> handle = this;
        finish(() -> {
            placeGroup.broadcastFlat(() -> {
                gen.accept(here(), handle);
            });
        });
    }

    @Override
    public TeamOperations<T, DistBag<T>> team() {
        return TEAM;
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistBag<>(pg1, id1);
        });
    }

}
