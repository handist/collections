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
import mpi.Op;

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

    /**
     * Gathers one boolean variable from all places, wrapping MPI#Allgather.
     *
     * @param val the value to gather.
     * @return an array storing gathered values to.
     */
    public boolean[] allGather1(boolean val) {
        final boolean[] send = new boolean[] { val };
        final boolean[] recv = new boolean[size];
        comm.Allgather(send, 0, 1, MPI.BOOLEAN, recv, 0, 1, MPI.BOOLEAN);
        return recv;
    }

    /**
     * Gathers one double variable from all places, wrapping MPI#Allgather.
     *
     * @param val the value to gather.
     * @return an array storing gathered values to.
     */
    public double[] allGather1(double val) {
        final double[] send = new double[] { val };
        final double[] recv = new double[size];
        comm.Allgather(send, 0, 1, MPI.DOUBLE, recv, 0, 1, MPI.DOUBLE);
        return recv;
    }

    /**
     * Gathers one float variable from all places, wrapping MPI#Allgather.
     *
     * @param val the value to gather.
     * @return an array storing gathered values to.
     */
    public float[] allGather1(float val) {
        final float[] send = new float[] { val };
        final float[] recv = new float[size];
        comm.Allgather(send, 0, 1, MPI.FLOAT, recv, 0, 1, MPI.FLOAT);
        return recv;
    }

    /**
     * Gathers one int variable from all places, wrapping MPI#Allgather.
     *
     * @param val the value to gather.
     * @return an array storing gathered values to.
     */
    public int[] allGather1(int val) {
        final int[] send = new int[] { val };
        final int[] recv = new int[size];
        comm.Allgather(send, 0, 1, MPI.INT, recv, 0, 1, MPI.INT);
        return recv;
    }

    /**
     * Gathers one long variable from all places, wrapping MPI#Allgather.
     *
     * @param val the value to gather.
     * @return an array storing gathered values to.
     */
    public long[] allGather1(long val) {
        final long[] send = new long[] { val };
        final long[] recv = new long[size];
        comm.Allgather(send, 0, 1, MPI.LONG, recv, 0, 1, MPI.LONG);
        return recv;
    }

    /**
     * Gathers one short variable from all places, wrapping MPI#Allgather.
     *
     * @param val tha value to gather.
     * @return an array storing gathered values to.
     */
    public short[] allGather1(short val) {
        final short[] send = new short[] { val };
        final short[] recv = new short[size];
        comm.Allgather(send, 0, 1, MPI.SHORT, recv, 0, 1, MPI.SHORT);
        return recv;
    }

    /**
     * Combine one boolean value of each process using the reduce operation, and
     * return the combined value of the all process.
     *
     * @param val send value.
     * @param op  reduce operation.
     * @return the combined value.
     */
    public boolean allReduce1(boolean val, Op op) {
        final boolean[] v = new boolean[] { val };
        comm.Allreduce(v, 0, v, 0, 1, MPI.BOOLEAN, op);
        return v[0];
    }

    /**
     * Combine one double value of each process using the reduce operation, and
     * return the combined value of the all process.
     *
     * @param val send value.
     * @param op  reduce operation.
     * @return the combined value.
     */
    public double allReduce1(double val, Op op) {
        final double[] v = new double[] { val };
        comm.Allreduce(v, 0, v, 0, 1, MPI.DOUBLE, op);
        return v[0];
    }

    /**
     * Combine one float value of each process using the reduce operation, and
     * return the combined value of the all process.
     *
     * @param val send value.
     * @param op  reduce operation.
     * @return the combined value.
     */
    public float allReduce1(float val, Op op) {
        final float[] v = new float[] { val };
        comm.Allreduce(v, 0, v, 0, 1, MPI.FLOAT, op);
        return v[0];
    }

    /**
     * Combine one int value of each process using the reduce operation, and return
     * the combined value of the all process.
     *
     * @param val send value.
     * @param op  reduce operation.
     * @return the combined value.
     */
    public int allReduce1(int val, Op op) {
        final int[] v = new int[] { val };
        comm.Allreduce(v, 0, v, 0, 1, MPI.INT, op);
        return v[0];
    }

    /**
     * Combine one long value of each process using the reduce operation, and return
     * the combined value of the all process.
     *
     * @param val send value.
     * @param op  reduce operation.
     * @return the combined value.
     */
    public long allReduce1(long val, Op op) {
        final long[] v = new long[] { val };
        comm.Allreduce(v, 0, v, 0, 1, MPI.LONG, op);
        return v[0];
    }

    /**
     * Combine one short value of each process using the reduce operation, and
     * return the combined value of the all process.
     *
     * @param val send value.
     * @param op  reduce operation.
     * @return the combined value.
     */
    public short allReduce1(short val, Op op) {
        final short[] v = new short[] { val };
        comm.Allreduce(v, 0, v, 0, 1, MPI.SHORT, op);
        return v[0];
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
     * Broadcast one boolean value from the root place to all processes of the
     * group.
     *
     * @param val  sent value from the root place. In other places, the value will
     *             be ignored.
     * @param root broadcast a value from the place.
     * @return If the root place, return val and if other places, return recieved
     *         value.
     */
    public boolean bCast1(boolean val, Place root) {
        final boolean[] v = new boolean[] { val };
        comm.Bcast(v, 0, 1, MPI.BOOLEAN, rank(root));
        return v[0];
    }

    /**
     * Broadcast one double value from the root place to all processes of the group.
     *
     * @param val  sent value from the root place. In other places, the value will
     *             be ignored.
     * @param root broadcast a value from the place.
     * @return If the root place, return val and if other places, return recieved
     *         value.
     */
    public double bCast1(double val, Place root) {
        final double[] v = new double[] { val };
        comm.Bcast(v, 0, 1, MPI.DOUBLE, rank(root));
        return v[0];
    }

    /**
     * Broadcast one float value from the root place to all processes of the group.
     *
     * @param val  sent value from the root place. In other places, the value will
     *             be ignored.
     * @param root broadcast a value from the place.
     * @return If the root place, return val and if other places, return recieved
     *         value.
     */
    public float bCast1(float val, Place root) {
        final float[] v = new float[] { val };
        comm.Bcast(v, 0, 1, MPI.FLOAT, rank(root));
        return v[0];
    }

    /**
     * Broadcast one int value from the root place to all processes of the group.
     *
     * @param val  sent value from the root place. In other places, the value will
     *             be ignored.
     * @param root broadcast a value from the place.
     * @return If the root place, return val and if other places, return recieved
     *         value.
     */
    public int bCast1(int val, Place root) {
        final int[] v = new int[] { val };
        comm.Bcast(v, 0, 1, MPI.INT, rank(root));
        return v[0];
    }

    /**
     * Broadcast one long value from the root place to all processes of the group.
     *
     * @param val  sent value from the root place. In other places, the value will
     *             be ignored.
     * @param root broadcast a value from the place.
     * @return If the root place, return val and if other places, return recieved
     *         value.
     */
    public long bCast1(long val, Place root) {
        final long[] v = new long[] { val };
        comm.Bcast(v, 0, 1, MPI.LONG, rank(root));
        return v[0];
    }

    /**
     * Broadcast one short value from the root place to all processes of the group.
     *
     * @param val  sent value from the root place. In other places, the value will
     *             be ignored.
     * @param root broadcast a value from the place.
     * @return If the root place, return val and if other places, return recieved
     *         value.
     */
    public short bCast1(short val, Place root) {
        final short[] v = new short[] { val };
        comm.Bcast(v, 0, 1, MPI.SHORT, rank(root));
        return v[0];
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

    /**
     * Gathers one boolean variable from all places, wrapping MPI#gather.
     *
     * @param val  the value to gather.
     * @param root gather values from all places to the root.
     * @return If at the root place, return an array storing gathered values to. At
     *         other places, return null.
     */
    public boolean[] gather1(boolean val, Place root) {
        final boolean[] v = new boolean[] { val };
        final boolean[] recv = new boolean[size];
        comm.Gather(v, 0, 1, MPI.BOOLEAN, recv, 0, 1, MPI.BOOLEAN, rank(root));
        return (root.equals(here())) ? recv : null;
    }

    /**
     * Gathers one double variable from all places, wrapping MPI#gather.
     *
     * @param val  the value to gather.
     * @param root gather values from all places to the root.
     * @return If at the root place, return an array storing gathered values to. At
     *         other places, return null.
     */
    public double[] gather1(double val, Place root) {
        final double[] v = new double[] { val };
        final double[] recv = new double[size];
        comm.Gather(v, 0, 1, MPI.DOUBLE, recv, 0, 1, MPI.DOUBLE, rank(root));
        return (root.equals(here())) ? recv : null;
    }

    /**
     * Gathers one float variable from all places, wrapping MPI#gather.
     *
     * @param val  the value to gather.
     * @param root gather values from all places to the root.
     * @return If at the root place, return an array storing gathered values to. At
     *         other places, return null.
     */
    public float[] gather1(float val, Place root) {
        final float[] v = new float[] { val };
        final float[] recv = new float[size];
        comm.Gather(v, 0, 1, MPI.FLOAT, recv, 0, 1, MPI.FLOAT, rank(root));
        return (root.equals(here())) ? recv : null;
    }

    /**
     * Gathers one int variable from all places, wrapping MPI#gather.
     *
     * @param val  the value to gather.
     * @param root gather values from all places to the root.
     * @return If at the root place, return an array storing gathered values to. At
     *         other places, return null.
     */
    public int[] gather1(int val, Place root) {
        final int[] v = new int[] { val };
        final int[] recv = new int[size];
        comm.Gather(v, 0, 1, MPI.INT, recv, 0, 1, MPI.INT, rank(root));
        return (root.equals(here())) ? recv : null;
    }

    /**
     * Gathers one long variable from all places, wrapping MPI#gather.
     *
     * @param val  the value to gather.
     * @param root gather values from all places to the root.
     * @return If at the root place, return an array storing gathered values to. At
     *         other places, return null.
     */
    public long[] gather1(long val, Place root) {
        final long[] v = new long[] { val };
        final long[] recv = new long[size];
        comm.Gather(v, 0, 1, MPI.LONG, recv, 0, 1, MPI.LONG, rank(root));
        return (root.equals(here())) ? recv : null;
    }

    /**
     * Gathers one short variable from all places, wrapping MPI#gather.
     *
     * @param val  the value to gather.
     * @param root gather values from all places to the root.
     * @return If at the root place, return an array storing gathered values to. At
     *         other places, return null.
     */
    public short[] gather1(short val, Place root) {
        final short[] v = new short[] { val };
        final short[] recv = new short[size];
        comm.Gather(v, 0, 1, MPI.SHORT, recv, 0, 1, MPI.SHORT, rank(root));
        return (root.equals(here())) ? recv : null;
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

    /**
     *
     * Combine one int value of each process using the reduce operation, and return
     * the combined value of the root process.
     *
     * @param val  send value.
     * @param op   reduce operation.
     * @param root combined value is returned only the root place.
     * @return at root place, the combined value. at other places, return val as it
     *         is.
     */
    public boolean reduce1(boolean val, Op op, Place root) {
        final boolean[] v = new boolean[] { val };
        comm.Reduce(v, 0, v, 0, 1, MPI.BOOLEAN, op, rank(root));
        return v[0];
    }

    /**
     *
     * Combine one int value of each process using the reduce operation, and return
     * the combined value of the root process.
     *
     * @param val  send value.
     * @param op   reduce operation.
     * @param root combined value is returned only the root place.
     * @return at root place, the combined value. at other places, return val as it
     *         is.
     */
    public double reduce1(double val, Op op, Place root) {
        final double[] v = new double[] { val };
        comm.Reduce(v, 0, v, 0, 1, MPI.DOUBLE, op, rank(root));
        return v[0];
    }

    /**
     *
     * Combine one float value of each process using the reduce operation, and
     * return the combined value of the root process.
     *
     * @param val  send value.
     * @param op   reduce operation.
     * @param root combined value is returned only the root place.
     * @return at root place, the combined value. at other places, return val as it
     *         is.
     */
    public float reduce1(float val, Op op, Place root) {
        final float[] v = new float[] { val };
        comm.Reduce(v, 0, v, 0, 1, MPI.FLOAT, op, rank(root));
        return v[0];
    }

    /**
     *
     * Combine one int value of each process using the reduce operation, and return
     * the combined value of the root process.
     *
     * @param val  send value.
     * @param op   reduce operation.
     * @param root combined value is returned only the root place.
     * @return at root place, the combined value. at other places, return val as it
     *         is.
     */
    public int reduce1(int val, Op op, Place root) {
        final int[] v = new int[] { val };
        comm.Reduce(v, 0, v, 0, 1, MPI.INT, op, rank(root));
        return v[0];
    }

    /**
     *
     * Combine one long value of each process using the reduce operation, and return
     * the combined value of the root process.
     *
     * @param val  send value.
     * @param op   reduce operation.
     * @param root combined value is returned only the root place.
     * @return at root place, the combined value. at other places, return val as it
     *         is.
     */
    public long reduce1(long val, Op op, Place root) {
        final long[] v = new long[] { val };
        comm.Reduce(v, 0, v, 0, 1, MPI.LONG, op, rank(root));
        return v[0];
    }

    /**
     *
     * Combine one short value of each process using the reduce operation, and
     * return the combined value of the root process.
     *
     * @param val  send value.
     * @param op   reduce operation.
     * @param root combined value is returned only the root place.
     * @return at root place, the combined value. at other places, return val as it
     *         is.
     */
    public short reduce1(short val, Op op, Place root) {
        final short[] v = new short[] { val };
        comm.Reduce(v, 0, v, 0, 1, MPI.SHORT, op, rank(root));
        return v[0];
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
