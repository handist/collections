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
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;

import apgas.Place;
import apgas.SerializableJob;
import apgas.mpi.MPILauncher;
import apgas.mpi.MPILauncher.Plugin;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import mpi.Comm;
import mpi.Datatype;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

/**
 * Represents a group of hosts and provides communication facilities between
 * hosts.
 * <p>
 * There is always one {@link TeamedPlaceGroup} initialized which contains all
 * the hosts involved in the computation. This instance can be obtained with
 * method {@link #getWorld()}.
 * <p>
 * When creating distributed collections, the hosts that will handle the
 * collections need to be specified with a {@link TeamedPlaceGroup} instance.
 * Such distributed collections can either operate with all the hosts involved
 * in the computation by using the the {@link TeamedPlaceGroup} returned by
 * {@link #getWorld()}, or with a subset of the hosts gathered in a new
 * {@link TeamedPlaceGroup} instance.
 */
public class TeamedPlaceGroup implements SerializableWithReplace {
    // TODO merge with ResilientPlaceGroup ?
    private static final class ObjectReference implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = -1948016251753684732L;
        private final GlobalID id;

        /**
         */
        private ObjectReference(GlobalID id) {
            this.id = id;
        }

        private Object readResolve() throws ObjectStreamException {
            return id.getHere();
        }
    }

    // TODO
    public static boolean debugF = false;

    static boolean isRegistered = false;

    static private volatile CountDownLatch readyToCloseWorld;

    static TeamedPlaceGroup world;

    public static TeamedPlaceGroup getWorld() {
        return world;
    }

    private static void readyToClose(boolean master) {
        if (master) {
            finish(() -> {
                world.broadcastFlat(() -> {
                    readyToCloseWorld.countDown();
                });
            });
        } else {
            try {
                readyToCloseWorld.await();
            } catch (final InterruptedException e) {
                System.err.println("[TeamedPlaceGroup#readyToClose] Error: readyToClose was interrupted at rank ["
                        + world.myrank + "]");
            }
        }
    }

    /**
     * Method that needs to be called on every host participating in the computation
     * before the {@link MPILauncher} is started. This method registers a "plugin"
     * with {@link MPILauncher#registerPlugin(Plugin)} which handles specific setup
     * needed by the distributed collection library.
     */
    public static void setup() {
        if (isRegistered) {
            return;
        }
        MPILauncher.registerPlugin(new Plugin() {
            @Override
            public void beforeFinalize(int rank, Comm comm) {
                readyToClose(rank == 0);
            }

            @Override
            public String getName() {
                return TeamedPlaceGroup.class.toString();
            }

            @Override
            public void init(int rank, Comm comm) throws MPIException {
                worldSetup();
            }
        });
        isRegistered = true;
    }

    /**
     * Called by {@link MPILauncher} through the plugin submitted in method
     * {@link #setup()}.
     *
     * @throws MPIException if such an exception is thrown during setup
     */
    private static void worldSetup() throws MPIException {
        final int myrank = MPI.COMM_WORLD.Rank();
        final int size = MPI.COMM_WORLD.Size();
        final int[] rank2place = new int[size];
        final Place here = here();
        if (debugF) {
            System.out.println("world setup: rank=" + myrank + ", place" + here + "::" + here.id);
        }
        rank2place[myrank] = here.id;
        MPI.COMM_WORLD.Allgather(rank2place, myrank, 1, MPI.INT, rank2place, 0, 1, MPI.INT);
        for (int i = 0; i < rank2place.length; i++) {
            if (debugF) {
                System.out.println("ws: " + i + ":" + rank2place[i] + "@" + myrank);
            }
        }
        GlobalID id;
        if (myrank == 0) { // we could use here() as an alternative
            id = new GlobalID();
            final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
            final ObjectOutput out = new ObjectOutput(out0);
            out.writeObject(id);
            out.close();
            final byte[] buf = out0.toByteArray();
            final int[] buf0 = new int[1];
            buf0[0] = buf.length;

            MPI.COMM_WORLD.Bcast(buf0, 0, 1, MPI.INT, 0);
            readyToCloseWorld = new CountDownLatch(1);
            MPI.COMM_WORLD.Bcast(buf, 0, buf0[0], MPI.BYTE, 0);
        } else {
            final int[] buf0 = new int[1];
            MPI.COMM_WORLD.Bcast(buf0, 0, 1, MPI.INT, 0);
            final byte[] buf = new byte[buf0[0]];
            readyToCloseWorld = new CountDownLatch(1);
            MPI.COMM_WORLD.Bcast(buf, 0, buf0[0], MPI.BYTE, 0);
            final ObjectInput in = new ObjectInput(new ByteArrayInputStream(buf));
            try {
                id = (GlobalID) in.readObject();
            } catch (final Exception e) {
                throw new Error("[TeamedPlaceGroup] init error at worker", e);
            } finally {
                in.close();
            }
        }
        world = new TeamedPlaceGroup(id, myrank, size, rank2place);
        /*
         * PlaceLocalObject.make(places(), ()->{ return new TeamedPlaceGroup().init();
         * });
         */
    }

    /**
     * Direct access to MPI functions is absolutely discouraged. This member will be
     * made private with intermediate access in the future.
     */
    /*
     * TODO this needs refactoring -> make this member private
     */
    @Deprecated
    public Intracomm comm;

    private final GlobalID id;
    final int myrank;

    private TeamedPlaceGroup parent;

    List<Place> places;

    // int[] place2rank;
    int size;

    /**
     * Proctected constructor used by class {@link SinglePlaceGroup} exclusively
     * <p>
     * This constructor initializes the members of {@link TeamedPlaceGroup} such
     * that a single place (the place on which this method is called) is contained
     * in the group.
     */
    protected TeamedPlaceGroup() {
        id = null;
        myrank = 0;
        size = 1;
        places = new ArrayList<>(size);
        comm = null;
        places.add(here());
    }

    /**
     * Constructor that builds a {@link TeamedPlaceGroup} instance which contains
     * all the hosts that participate in the computation
     *
     * @param id         a global id for this handle
     * @param myrank     the rank of this host in the world
     * @param size       number of hosts in the world
     * @param rank2place correspondance array between {@link Place} number and MPI
     *                   rank
     */
    protected TeamedPlaceGroup(GlobalID id, int myrank, int size, int[] rank2place) { // for whole_world
        this.id = id;
        this.size = size;
        this.myrank = myrank;
        places = new ArrayList<>(size);
        comm = MPI.COMM_WORLD;
        // this.place2rank = new int[size];
        for (int i = 0; i < rank2place.length; i++) {
            final int p = rank2place[i];
            places.add(new Place(p));
            // place2rank[p] = i;
        }
        id.putHere(this);
        parent = null;
    }

    protected TeamedPlaceGroup(GlobalID id, int myrank, List<Place> places, Intracomm comm, TeamedPlaceGroup parent) {
        this.id = id;
        size = places.size();
        this.myrank = myrank;
        this.comm = comm;
        this.places = places;
        this.parent = parent;
        id.putHere(this);
    }

    @SuppressWarnings("unused")
    public void Alltoallv(Object byteArray, int soffset, int[] sendSize, int[] sendOffset, Datatype stype,
            Object recvbuf, int roffset, int[] rcvSize, int[] rcvOffset, Datatype rtype) throws MPIException {
        if (false) {
            comm.Alltoallv(byteArray, soffset, sendSize, sendOffset, stype, recvbuf, roffset, rcvSize, rcvOffset,
                    rtype);
        } else {
            for (int rank = 0; rank < rcvSize.length; rank++) {
                comm.Gatherv(byteArray, soffset + sendOffset[rank], sendSize[rank], stype, recvbuf, roffset, rcvSize,
                        rcvOffset, rtype, rank);
            }
        }
    }

    public void barrier() {
        try {
            comm.Barrier();
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("[TeamedPlaceGroup] MPI Exception raised.");
        }
    }

    /**
     * Makes the specified job run on all the hosts of this {@link TeamedPlaceGroup}
     * and returns when this it has terminated on all the hosts.
     *
     * @param job the job to run
     */
    public void broadcastFlat(SerializableJob job) {
        // TODO
        finish(() -> {
            for (final Place p : places()) {
                if (!p.equals(here())) {
                    asyncAt(p, job);
                }
            }
            job.run();
        });
    }

    public Place get(int rank) {
        return places.get(rank);
    }

    /**
     * Returns the "parent" of this {@link TeamedPlaceGroup}, or {@code null} if
     * there is no such parent
     *
     * @return the parent of this {@link TeamedPlaceGroup}
     */
    public TeamedPlaceGroup getParent() {
        return parent;
    }

    /**
     * TODO is this method redundant? should we delete it?
     *
     * @return this
     */
    protected TeamedPlaceGroup init() {
        // TODO
        // setup MPI
        /*
         * if(!MPI.Initialized()) { throw new
         * Error("[TeamedPlaceGroup] Please setup MPI first"); }
         */
        // setup arrays
        // setup rank2place
        // share the infromation
        // set this to singleton
        return this;
    }

    public List<Place> places() {
        return places;
    }

    /**
     * Returns the MPI rank of the calling host in the current
     * {@link TeamedPlaceGroup}.
     *
     * @return rank of this host within the {@link TeamedPlaceGroup}
     */
    public int rank() {
        return myrank;
    }

    /**
     * Returns the MPI rank of the specified place in the current
     * {@link TeamedPlaceGroup}. If the specified place is not a member of this
     * {@link TeamedPlaceGroup}, throws a {@link RuntimeException}.
     *
     * @param place place whose rank is to be returned
     * @return rank of this host within the {@link TeamedPlaceGroup}
     * @throws RuntimeException if the specified place is not a member of this group
     */
    public int rank(Place place) {
        final int result = places.indexOf(place);
        if (result < 0) {
            throw new RuntimeException("[TeamedPlaceGroup] " + place + " is not a member of " + this);
        }
        return result;
    }

    // TODO
    // split, relocate feature
    public void remove(GlobalID id) {
        // TODO

    }

    /**
     * Returns the number of hosts that are members of this
     * {@link TeamedPlaceGroup}.
     *
     * @return number of hosts in the group
     */
    public int size() {
        return size;
    }

    public TeamedPlaceGroup split(SortedMap<Integer, Integer> rank2color) {
        try {
            final int newColor = rank2color.get(myrank);
            int newRank = 0;
            final List<Place> newPlaces = new ArrayList<>();
            for (final Map.Entry<Integer, Integer> entry : rank2color.entrySet()) {
                final int r = entry.getKey();
                if (entry.getValue().equals(newColor)) {
                    if (r == myrank) {
                        newRank = newPlaces.size();
                    }
                    newPlaces.add(places.get(r));
                }
            }
            final Intracomm newComm = comm.Split(newColor, newRank); // MPIException
            if (debugF) {
                System.out.println("PlaceGroup split =" + newRank + ", place" + here() + "::" + here().id);
            }
            GlobalID id;
            if (newRank == 0) {
                id = new GlobalID();
                final ByteArrayOutputStream out0 = new ByteArrayOutputStream();
                final ObjectOutput out = new ObjectOutput(out0);
                out.writeObject(id);
                out.close();
                final byte[] buf = out0.toByteArray();
                final int[] buf0 = new int[1];
                buf0[0] = buf.length;
                newComm.Bcast(buf0, 0, 1, MPI.INT, 0);
                newComm.Bcast(buf, 0, buf0[0], MPI.BYTE, 0);
            } else {
                final int[] buf0 = new int[1];
                newComm.Bcast(buf0, 0, 1, MPI.INT, 0);
                final byte[] buf = new byte[buf0[0]];
                newComm.Bcast(buf, 0, buf0[0], MPI.BYTE, 0);
                final ObjectInput in = new ObjectInput(new ByteArrayInputStream(buf));
                try {
                    id = (GlobalID) in.readObject();
                } catch (final Exception e) {
                    throw new Error("[TeamedPlaceGroup] init error at worker");
                } finally {
                    in.close();
                }
            }
            return new TeamedPlaceGroup(id, newRank, newPlaces, newComm, this);
            /*
             * PlaceLocalObject.make(places(), ()->{ return new TeamedPlaceGroup().init();
             * });
             */
        } catch (final MPIException e) {
            throw new RuntimeException("[TeamedPlaceGroup] MPIException caught.");
        }
    }
    /*
     * TODO: Is close() needed? What close() should do? public void close() {
     * comm.Free(); }
     */

    /**
     * Creates a new TeamedPlaceGroup from this instance containing half of the
     * places involved in this instance
     *
     * @return a new TeamedPlaceGroup
     */
    public TeamedPlaceGroup splitHalf() {
        final TreeMap<Integer, Integer> rank2color = new TreeMap<>();
        if (size() == 1) {
            throw new RuntimeException(
                    "[TeamedPlaceGroup] TeamedPlaceGroup with size == 1 cannnot be split any further");
        }
        final int half = size() / 2;
        for (int i = 0; i < half; i++) {
            rank2color.put(i, 0);
        }
        for (int i = half; i < size(); i++) {
            rank2color.put(i, 1);
        }
        return split(rank2color);
    }

    @Override
    public String toString() {
        return "TeamedPlaceGroup[" + id + ", myrank:" + myrank + ", places:" + places() + "]";
    }

    public Object writeReplace() throws ObjectStreamException {
        return new ObjectReference(id);
    }
}
