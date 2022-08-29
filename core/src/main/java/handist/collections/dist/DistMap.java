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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.ParallelMap;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;
import handist.collections.glb.DistMapGlb;

/**
 * A Map data structure spread over the multiple places.
 *
 * @param <K> type of the key used in the {@link DistMap}
 * @param <V> type of the value mapped to each key in the {@link DistMap}
 */
public class DistMap<K, V> extends ParallelMap<K, V>
        implements DistributedCollection<V, DistMap<K, V>>, KeyRelocatable<K>, SerializableWithReplace {

    // TODO
//     public <T, U> void setupBranches(Generator<T,U> gen) {
//         final DistMap<T,U> handle = this;
//         finish(()->{
//             placeGroup.broadcastFlat(()->{
//                 gen.accept(here(), handle);
//             });
//         });
//     }

//  Method moved to TEAM and GLOBAL operations
//  @Override
//  public void distSize(long[] result) {
//      TeamedPlaceGroup pg = this.placeGroup;
//      long localSize = data.size(); // int->long
//      long[] sendbuf = new long[] { localSize };
//      // team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
//      try {
//          pg.comm.Allgather(sendbuf, 0, 1, MPI.LONG, result, 0, 1, MPI.LONG);
//      } catch (MPIException e) {
//          e.printStackTrace();
//          throw new Error("[DistMap] network error in balance()");
//      }
//  }

    private static int _debug_level = 5;

    /** Handle for GLB operations */
    public final DistMapGlb<K, V> GLB;
    public GlobalOperations<V, DistMap<K, V>> GLOBAL;

    final GlobalID id;

    public transient float[] locality;

    public final TeamedPlaceGroup placeGroup;

    protected final TeamOperations<V, DistMap<K, V>> TEAM;

    @SuppressWarnings("rawtypes")
    private DistCollectionSatellite satellite;

    /**
     * Construct an empty DistMap which can have local handles on all the hosts in
     * the computation.
     */
    public DistMap() {
        this(TeamedPlaceGroup.world);
    }

    /**
     * Construct a DistMap which can have local handles on the hosts of the
     * specified {@link TeamedPlaceGroup}.
     *
     * @param pg the group of hosts that are susceptible to manipulate this
     *           {@link DistMap}
     */
    public DistMap(TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }

    DistMap(TeamedPlaceGroup pg, GlobalID globalID) {
        this(pg, globalID, new HashMap<>());
    }

    /**
     * Package private DistMap constructor. This constructor is used to register a
     * new DistMap handle with the specified GlobalId. Programmers that use this
     * library should never have to call this constructor.
     * <p>
     * Specifying a GLobalId which already has object handles registered in other
     * places (potentially objects different from a {@link DistMap} instance) could
     * prove disastrous. Instead, programmers should only call {@link #DistMap()} to
     * create a distributed map with handles on all hosts, or
     * {@link #DistMap(TeamedPlaceGroup)} to restrict their DistMap to a subset of
     * hosts.
     *
     * @param pg       the palceGroup on which this DistMap is defined
     * @param globalId the global id associated to this distributed map
     * @param data     the data container to be used
     */
    DistMap(TeamedPlaceGroup pg, GlobalID globalId, Map<K, V> data) {
        super(data);
        placeGroup = pg;
        id = globalId;
        locality = new float[pg.size];
        Arrays.fill(locality, 1.0f);
        this.GLOBAL = new GlobalOperations<>(this, (TeamedPlaceGroup pg0, GlobalID gid) -> new DistMap<>(pg0, gid));
        GLB = new DistMapGlb<>(this);
        TEAM = new TeamOperations<>(this);
        id.putHere(this);
    }

    @Override
    public Collection<K> getAllKeys() {
        return keySet();
    }

    /**
     * Returns a subset of the keys contained in the local map. If the specified
     * number of keys is greater than the number of keys actually contained in the
     * local map, the entire keyset is returned. If a nil or negative number of keys
     * is asked for, an empty collection is returned.
     *
     * @param count number of keys desired
     * @return a collection containing the specified number of keys, or less if the
     *         local map contains fewer keys than the specified parameter
     */
    private Collection<K> getNKeys(int count) {
        if (count <= 0) {
            return Collections.emptySet();
        }
        final ArrayList<K> keys = new ArrayList<>();
        for (final K key : data.keySet()) {
            keys.add(key);
            --count;
            if (count == 0) {
                return keys;
            }
        }
        return data.keySet();
    }

    /**
     * Return new {@link MapEntryDispatcher} instance that enable fast relocation
     * between places than normal.
     *
     * @param rule Determines the dispatch destination.
     * @return :
     */
    public MapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule) {
        return new MapEntryDispatcher<>(this, placeGroup(), rule);
    }

    /**
     * Return new {@link MapEntryDispatcher} instance that enable fast relocation
     * between places than normal.
     *
     * @param rule Determines the dispatch destination.
     * @param pg   Relocate in this placegroup.
     * @return :
     * @throws IllegalArgumentException :
     */
    public MapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule, TeamedPlaceGroup pg)
            throws IllegalArgumentException {
        if (placeGroup.places.containsAll(pg.places)) {
            throw new IllegalArgumentException("The TeamedlaceGroup passed to DistMapDispatcher must be part of or "
                    + "the same as TeamedPlaceGroup in origin DistMap.");
        }
        return new MapEntryDispatcher<>(this, pg, rule);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends DistCollectionSatellite<DistMap<K, V>, S>> S getSatellite() {
        return (S) satellite;
    }

    @Override
    public GlobalOperations<V, DistMap<K, V>> global() {
        return GLOBAL;
    }

    @Override
    public GlobalID id() {
        return id;
    }

    @Override
    public float[] locality() {
        return locality;
    }

    @Override
    public long longSize() {
        return data.size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void moveAtSync(Collection<K> keys, Place pl, MoveManager mm) {
        if (pl.equals(Constructs.here())) {
            return;
        }
        final DistMap<K, V> collection = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final int size = keys.size();
            s.writeInt(size);
            for (final K key : keys) {
                final V value = collection.remove(key);
                s.writeObject(key);
                s.writeObject(value);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int size = ds.readInt();
            for (int i = 1; i <= size; i++) {
                final K key = (K) ds.readObject();
                final V value = (V) ds.readObject();
                collection.putForMove(key, value);
            }
        };
        mm.request(pl, serialize, deserialize);
    }

    @Override
    public void moveAtSync(Function<K, Place> rule, MoveManager mm) {
        final DistMap<K, V> collection = this;
        final HashMap<Place, List<K>> keysToMove = new HashMap<>();
        collection.forEach((K key, V value) -> {
            final Place destination = rule.apply(key);
            if (!keysToMove.containsKey(destination)) {
                keysToMove.put(destination, new ArrayList<K>());
            }
            keysToMove.get(destination).add(key);
        });
        for (final Map.Entry<Place, List<K>> entry : keysToMove.entrySet()) {
            moveAtSync(entry.getValue(), entry.getKey(), mm);
        }
    }

    /**
     * Request that the specified element is relocated when #sync is called.
     *
     * @param key the key of the relocated entry.
     * @param pl  the destination place.
     * @param mm  MoveManagerLocal
     */
    @Override
    @SuppressWarnings("unchecked")
    public void moveAtSync(K key, Place pl, MoveManager mm) {
        if (pl.equals(Constructs.here())) {
            return;
        }
        final DistMap<K, V> toBranch = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final V value = this.remove(key);
            s.writeObject(key);
            s.writeObject(value);
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final K k = (K) ds.readObject();
            final V v = (V) ds.readObject();
            toBranch.putForMove(k, v);
        };
        mm.request(pl, serialize, deserialize);
    }

    /*
     * void teamedBalance() { LoadBalancer.MapBalancer<T, U> balance = new
     * LoadBalancer.MapBalancer<>(this.data, placeGroup); balance.execute();
     * if(debugPrint()) System.out.println(here() + " balance.check1"); clear();
     * if(debugPrint()) { System.out.println(here() + " balance.check2");
     * System.out.println(here() + " balance.ArrayList.size() : " + data.size()); }
     * long time = - System.nanoTime(); time += System.nanoTime(); if(debugPrint())
     * { // System.out.println(here() + " count : " + (count) + " ms"); //
     * System.out.println(here() + " put : " + (total/(1000000)) + " ms");
     * System.out.println(here() + " for : " + (time/(1000000)) + " ms");
     * System.out.println(here() + " data.size() : " + size());
     * System.out.println(here() + " balance.check3"); }
     *
     * }
     */

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

    // TODO different naming convention of balance methods with DistMap

    public void moveAtSyncCount(int count, Place dest, MoveManager mm) {
        if (count == 0) {
            return;
        }
        moveAtSync(getNKeys(count), dest, mm);
    }

    /*
     * Abstractovdef create(placeGroup: PlaceGroup, team: TeamOperations, init:
     * ()=>Map[T, U]){ // return new DistMap[T,U](placeGroup, init) as
     * AbstractDistCollection[Map[T,U]]; return null as
     * AbstractDistCollection[Map[T,U]]; }
     */
    /*
     * public def versioningMap(srcName : String){ // return new
     * BranchingManager[DistMap[T,U], Map[T,U]](srcName, this); return null as
     * BranchingManager[DistMap[T,U], Map[T,U]]; }
     */

    @Override
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    protected V putForMove(K key, V value) {
        if (data.containsKey(key)) {
            throw new RuntimeException("DistMap cannot override existing entry: " + key);
        }
        return data.put(key, value);
    }

    /**
     * Reduce the all elements including other place using the given operation.
     *
     * @param <S>  type of the result produced by the reduction operation
     * @param lop  the operation using in the local reduction.
     * @param gop  the operation using in the reduction of the results of the local
     *             reduction.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public <S> S reduce(BiFunction<S, V, S> lop, BiFunction<S, S, S> gop, S unit) {
        // TODO
        throw new Error("Not implemented yet.");
        /*
         * val reducer = new Reducible[S]() { public def zero() = unit; public operator
         * this(a: S, b: S) = gop(a, b); }; return finish (reducer) {
         * placeGroup.broadcastFlat(() => { offer(reduceLocal(lop, unit)); }); };
         */
    }

    /**
     * Reduce the all elements including other place using the given operation.
     *
     * @param op   the operation.
     * @param unit the neutral element of the reduction.
     * @return the result of the reduction.
     */
    public V reduce(BiFunction<V, V, V> op, V unit) {
        return reduce(op, op, unit);
    }

    @Override
    public void relocate(Distribution<K> rule) throws Exception {
        relocate(rule, new CollectiveMoveManager(placeGroup));
    }

    @Override
    public void relocate(Distribution<K> rule, CollectiveMoveManager mm) throws Exception {
        for (final K key : data.keySet()) {
            final Place place = rule.location(key);
            if (place == null) {
                throw new NullPointerException("DistMap.relocate must not relocate entries to null place");
            }
            moveAtSync(key, place, mm);
        }
        mm.sync();
    }

    @Override
    public void relocate(Function<K, Place> rule) throws Exception {
        relocate(rule, new CollectiveMoveManager(placeGroup));
    }

    @Override
    public void relocate(Function<K, Place> rule, CollectiveMoveManager mm) throws Exception {
        for (final K key : data.keySet()) {
            final Place place = rule.apply(key);
            moveAtSync(key, place, mm);
        }
        mm.sync();
    }

    @Override
    public <S extends DistCollectionSatellite<DistMap<K, V>, S>> void setSatellite(S s) {
        satellite = s;
    }

    @Override
    public TeamOperations<V, DistMap<K, V>> team() {
        return TEAM;
    }

    @Override
    public String toString() {
        final StringWriter out0 = new StringWriter();
        final PrintWriter out = new PrintWriter(out0);
        out.println("at " + here());
        for (final Map.Entry<K, V> e : data.entrySet()) {
            out.println("key : " + e.getKey() + ", value : " + e.getValue());
        }
        out.close();
        return out0.toString();
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistMap<>(pg1, id1);
        });
    }

}
