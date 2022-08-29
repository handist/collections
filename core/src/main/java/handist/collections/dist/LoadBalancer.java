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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

import apgas.Place;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import mpi.MPI;
import mpi.MPIException;

//TODO not used now.
// for internal use
// this is a class for the load balancing
@SuppressWarnings("deprecation")
abstract class LoadBalancer {

    // private static Place tmpRoot = Place(0);

    static final class ListBalancer<T> extends LoadBalancer {

        private final List<T> body;

        public ListBalancer(List<T> body, TeamedPlaceGroup pg) {
            super(pg);
            this.body = body;
        }

        @Override
        void exportOne(ObjectOutput out) throws IOException {
            final T one = body.remove(body.size() - 1);
            out.writeObject(one);
        }

        @Override
        @SuppressWarnings("unchecked")
        void importOne(ObjectInput in) throws ClassNotFoundException, IOException {
            body.add((T) in.readObject());
        }

        @Override
        int localSize() {
            return body.size();
        }

    }

    static final class MapBalancer<K, V> extends LoadBalancer {

        private final Map<K, V> body;

        public MapBalancer(Map<K, V> body, TeamedPlaceGroup pg) {
            super(pg);
            this.body = body;
        }

        @Override
        void exportOne(ObjectOutput out) throws IOException {
            assert (!body.isEmpty());
            final K key = body.keySet().iterator().next();
            final V v = body.remove(key);
            out.writeObject(key);
            out.writeObject(v);
        }

        @Override
        @SuppressWarnings("unchecked")
        void importOne(ObjectInput obj) throws ClassNotFoundException, IOException {
            final K key = (K) obj.readObject();
            final V v = (V) obj.readObject();
            body.put(key, v);
        }

        @Override
        int localSize() {
            return body.size();
        }

    }

    private final int myRole;

    private final TeamedPlaceGroup pg;
    private final ArrayList<Integer> receivers;
    private final Place root;
    private final ArrayList<Integer> senders;

    public LoadBalancer(/* List<T> list, */ TeamedPlaceGroup pg) {
        // this.list = list;
        this.pg = pg;
        root = pg.get(0);
        myRole = pg.rank(here());
        senders = new ArrayList<>(pg.size());
        receivers = new ArrayList<>(pg.size());
    }

    public void execute() {
        if (pg.size() == 1) {
            return;
        }
        try {
            relocate(getMoveCount());
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("MPI Exception");
        }
    }

    abstract void exportOne(ObjectOutput out) throws IOException;

    // return (fromId, toId) => moveCount
    private BiFunction<Integer, Integer, Integer> getMoveCount() throws MPIException {
        final int np = pg.size();
        final int[] matrix = new int[np * np];
        senders.clear();
        receivers.clear();
        final long[] tmpOverCounts = new long[np];
        Arrays.fill(tmpOverCounts, localSize());
        final long[] overCounts = new long[np];
        // team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
        pg.comm.allToAll(tmpOverCounts, 1, MPI.LONG, overCounts, 1, MPI.LONG);
        long total = 0;
        for (int i = 0; i < np; i++) {
            total = total + overCounts[i];
        }
        final long average = total / np;
        for (int i = 0; i < np; i++) {
            overCounts[i] = overCounts[i] - average;
        }
        for (int i = 0; i < np; i++) {
            final long overCount = overCounts[i];
            if (overCount < 0) {
                receivers.add(i);
            } else if (overCount > 0) {
                senders.add(i);
            }
        }
        if ((here().equals(root)) && (0 < senders.size()) && (0 < receivers.size())) {
            final Integer[] sendersX = new Integer[senders.size()];
            final Integer[] receiversX = new Integer[receivers.size()];
            senders.toArray(sendersX);
            final Random random = new Random();
            for (int i = 0; i < sendersX.length; i++) {
                final Integer j = random.nextInt(sendersX.length);
                final Integer tmp = sendersX[j];
                sendersX[j] = sendersX[i];
                sendersX[i] = tmp;
            }
            receivers.toArray(receiversX);
            final Comparator<Integer> comp = (Integer a, Integer b) -> {
                return Long.compare(overCounts[b], overCounts[a]);
            };
            Arrays.sort(receiversX, comp);
            int senderPointer = 0;
            int receiverPointer = 1;
            while ((receiverPointer < receiversX.length) && (senderPointer < sendersX.length)) {
                final int i = sendersX[senderPointer];
                final int j = receiversX[receiverPointer - 1];
                final int k = receiversX[receiverPointer];
                while ((overCounts[k] < overCounts[j]) && (0 < overCounts[i])) {
                    overCounts[i]--;
                    overCounts[k]++;
                    matrix[np * i + k]++;
                }
                if (overCounts[j] == overCounts[k]) {
                    receiverPointer++;
                }
                if (overCounts[i] == 0) {
                    senderPointer++;
                }
            }
            while (senderPointer < sendersX.length) {
                final int i = sendersX[senderPointer];
                while (0 < overCounts[i]) {
                    receiverPointer = (receiverPointer + 1) % receiversX.length;
                    final int j = receiversX[receiverPointer];
                    overCounts[i]--;
                    overCounts[j]++;
                    matrix[np * i + j]++;
                }
                senderPointer++;
            }
        }
        // team.bcast(tmpRoot, matrix, 0, matrix, 0, np * np);
        pg.comm.bcast(matrix, np * np, MPI.INT, pg.rank(root));
        final BiFunction<Integer, Integer, Integer> func = (Integer i0, Integer j0) -> {
            return matrix[np * i0 + j0];
        };
        return func;
    }

    abstract void importOne(ObjectInput in) throws ClassNotFoundException, IOException;

    // private List<T> list;
    abstract int localSize();

    // execute relocation using getCount function
    private void relocate(BiFunction<Integer, Integer, Integer> getCount) {
        try {
            final int np = pg.size();
            final ByteArrayOutputStream s0 = new ByteArrayOutputStream();
            final int[] scounts = new int[np];
            final int[] sdispls = new int[np];
            final int[] rcounts = new int[np];
            int s0used = 0;
            for (int j = 0; j < np; j++) {
                final int count = getCount.apply(myRole, j);
                if (count > 0) {
                    final ObjectOutput s = new ObjectOutput(s0);
                    s.writeInt(count);
                    for (int k = 0; k < count; k++) {
                        exportOne(s);
                    }
                    s.close();
                    final int prev = s0used;
                    sdispls[j] = prev;
                    s0used = s0.size();
                    scounts[j] = s0used - prev;
                } else {
                    sdispls[j] = s0used;
                    scounts[j] = 0;
                }
            }

            pg.comm.allToAll(scounts, 1, MPI.INT, rcounts, 1, MPI.INT);
            final byte[] sendbuf = s0.toByteArray();

            final int[] rdispls = new int[np];
            int rused = 0;
            for (int i = 0; i < np; i++) {
                rdispls[i] = rused;
                rused += rcounts[i];
            }
            final byte[] recvbuf = new byte[rused];
            pg.Alltoallv(sendbuf, scounts, sdispls, MPI.BYTE, recvbuf, rcounts, rdispls, MPI.BYTE);

            for (int i = 0; i < np; i++) {
                if (rcounts[i] == 0) {
                    continue;
                }
                final ByteArrayInputStream in = new ByteArrayInputStream(recvbuf, rdispls[i], rcounts[i]);
                final ObjectInput ds = new ObjectInput(in);
                final int count = ds.readInt();
                assert (getCount.apply(i, myRole) == count);
                for (int k = 0; k < count; k++) {
                    importOne(ds);
                }
                ds.close();
            }
        } catch (final Exception e) {
            e.printStackTrace(System.err);
            throw new Error("Exception during LoadBalance Relocation.");
        }
    }
}
/*
 * // test class Main {
 *
 * public static def main(args: Rail[String]): void { val o = new Main();
 * o.run(); }
 *
 * def run(): void { val pg = Place.places(); val team = TeamOperations(pg);
 * pg.broadcastFlat(() => { val executor = new Executor(pg, team);
 * executor.start(); }); }
 *
 * static class Executor(pg: PlaceGroup, team: TeamOperations) {
 *
 * transient var map: x10.util.Map[Long, String];
 *
 * def start(): void { initialize(() => 1000000); balance(); }
 *
 * def initialize(count: ()=>Long): void { val begin = System.nanoTime(); if
 * (map == null) { map = new x10.util.HashMap[Long, String](); } val num =
 * count(); for (var i: Long = 0; i < num; i++) { map(num * here.id + i) =
 * i.toString(); } val end = System.nanoTime(); System.out.println(here +
 * " initialize " + ((end - begin) * 1e-6) + " ms"); }
 *
 * def balance(): void { val begin = System.nanoTime(); val al = new
 * ArrayList[x10.util.Map.Entry[Long, String]](map.size());
 * al.addAll(map.entries()); map.clear(); val balancer = new
 * LoadBalancer[x10.util.Map.Entry[Long, String]](al, pg, team);
 * balancer.execute(); for (e in al) { map(e.getKey()) = e.getValue(); } val end
 * = System.nanoTime(); System.out.println(here + " balance " + ((end - begin) *
 * 1e-6) + " ms"); } } }
 */
