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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.Serializer;
import handist.collections.glb.DistMapGlb;
import mpjbuf.IllegalArgumentException;

/**
 * A Map data structure spread over the multiple places.
 *
 * @param <K> type of the key used in the {@link DistMap}
 * @param <V> type of the value mapped to each key in the {@link DistMap}
 */
public class DistMap<K, V>
        implements Map<K, V>, DistributedCollection<V, DistMap<K, V>>, KeyRelocatable<K>, SerializableWithReplace {

    // TODO
    /*
     * public void setupBranches(Generator<T,U> gen) { final DistMap<T,U> handle =
     * this; finish(()->{ placeGroup.broadcastFlat(()->{ gen.accept(here(), handle);
     * }); }); }
     */

    // TODO implements Relocatable

    private static int _debug_level = 5;

    /**
     * Implementation of the local Map collection
     */
    protected Map<K, V> data;
    /** Handle for GLB operations */
    public final DistMapGlb<K, V> GLB;
    public GlobalOperations<V, DistMap<K, V>> GLOBAL;

    final GlobalID id;

    public transient float[] locality;

    public final TeamedPlaceGroup placeGroup;

    private Function<K, V> proxyGenerator;

    protected final TeamOperations<V, DistMap<K, V>> TEAM;

    @SuppressWarnings("rawtypes")
    private DistCollectionSatellite satellite;

    private final MapEntryDispatcher<K, V> dispatcher;

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

//	Method moved to TEAM and GLOBAL operations
//	@Override
//	public void distSize(long[] result) {
//		TeamedPlaceGroup pg = this.placeGroup;
//		long localSize = data.size(); // int->long
//		long[] sendbuf = new long[] { localSize };
//		// team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
//		try {
//			pg.comm.Allgather(sendbuf, 0, 1, MPI.LONG, result, 0, 1, MPI.LONG);
//		} catch (MPIException e) {
//			e.printStackTrace();
//			throw new Error("[DistMap] network error in balance()");
//		}
//	}

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
        placeGroup = pg;
        id = globalId;
        locality = new float[pg.size];
        Arrays.fill(locality, 1.0f);
        this.data = data;
        this.GLOBAL = new GlobalOperations<>(this, (TeamedPlaceGroup pg0, GlobalID gid) -> new DistMap<>(pg0, gid));
        GLB = new DistMapGlb<>(this);
        TEAM = new TeamOperations<>(this);
        dispatcher = new MapEntryDispatcher<>(this, null);
        id.putHere(this);
    }

    /**
     * Remove the all local entries.
     */
    @Override
    public void clear() {
        data.clear();
    }

    /**
     * Return true if the specified entry is exist in the local collection.
     *
     * @param key a key.
     * @return true is the specified object is a key present in the local map,
     */
    @Override
    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    /**
     * Indicates if the provided value is contained in the local map.
     */
    @Override
    public boolean containsValue(Object value) {
        return data.containsValue(value);
    }

    boolean debugPrint() {
        return true;
    }

    /**
     * Removes the provided key from the local map, returns {@code true} if there
     * was a previous obejct mapped to this key, {@code false} if there were no
     * mapping with this key or if the mapping was a {@code null} object
     *
     * @param key the key to remove from this local map
     * @return true if a mapping was removed as a result of this operation, false
     *         otherwise
     */
    public boolean delete(K key) {
        final V result = data.remove(key);
        return (result != null);
    }

    /**
     * Return the Set of local entries.
     *
     * @return the Set of local entries.
     */
    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return data.entrySet();
    }

    /**
     * Apply the specified operation with each Key/Value pair contained in the local
     * collection.
     *
     * @param action the operation to perform
     */
    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        if (!data.isEmpty()) {
            data.forEach(action);
        }
    }

    @Override
    public void forEach(SerializableConsumer<V> action) {
        data.values().forEach(action);
    }

    private void forEachParallelBodyLocal(SerializableConsumer<V> action) {
        final List<Collection<V>> separated = separateLocalValues(Runtime.getRuntime().availableProcessors() * 2);
        for (final Collection<V> sub : separated) {
            async(() -> {
                sub.forEach(action);
            });
        }
    }

    /**
     * Helper method which separates the keys contained in the local map into even
     * batches for the number of threads available on the system and applies the
     * provided action on each key/value pair contained in the collection in
     * parallel
     *
     * @param action action to perform on the key/value pair contained in the map
     */
    private void forEachParallelKey(SerializableBiConsumer<? super K, ? super V> action) {
        final int batches = Runtime.getRuntime().availableProcessors();

        // Dispatch the existing keys into batches
        final List<Collection<K>> keys = new ArrayList<>(batches);
        for (int i = 0; i < batches; i++) {
            keys.add(new HashSet<>());
        }
        // Round-robin of keys into batches
        int i = 0;
        for (final K k : data.keySet()) {
            keys.get(i++).add(k);
            if (i >= batches) {
                i = 0;
            }
        }

        // Spawn asynchronous activity for each batch
        for (final Collection<K> keysToProcess : keys) {
            async(() -> {
                // Apply the supplied action on each key in the batch
                for (final K key : keysToProcess) {
                    final V value = data.get(key);
                    action.accept(key, value);
                }
            });
        }
    }

    /**
     * Return the element for the provided key. If there is no element at the index,
     * return null.
     *
     * When an agent generator is set on this instance and there is no element at
     * the index, a proxy value for the index is generated as a return value.
     *
     * @param key the index of the value to retrieve
     * @return the element associated with {@code key}.
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        final V result = data.get(key);
        if (result != null) {
            return result;
        }
        if (proxyGenerator != null && !data.containsKey(key)) {
            return proxyGenerator.apply((K) key);
        } else {
            return null;
        }
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
     * Return {@link MapEntryDispatcher} instance that enable fast relocation
     * between places than normal. One {@link DistMap} has one dispatcher.
     *
     * @param rule Determines the dispatch destination.
     * @return :
     */
    public MapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule) {
        dispatcher.setDistribution(rule);
        return dispatcher;
    }

    /**
     * Return {@link MapEntryDispatcher} instance that enable fast relocation
     * between places than normal. One {@link DistMap} has one dispatcher.
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

    // TODO remove this method, we already have #putAll
    public void integrate(Map<K, V> src) {
        for (final Map.Entry<K, V> e : src.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    /**
     * Indicates if the local distributed map is empty or not
     *
     * @return {@code true} if there are no mappings in the local map
     */
    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Return the Set of local keys.
     *
     * @return the Set of local keys.
     */
    @Override
    public Set<K> keySet() {
        return data.keySet();
    }

    @Override
    public float[] locality() {
        return locality;
    }

    @Override
    public long longSize() {
        return data.size();
    }

    /**
     * Apply the same operation on the all elements including remote places and
     * creates a new {@link DistMap} with the same keys as this instance and the
     * result of the mapping operation as values.
     *
     * @param <W> result type of mapping operation
     * @param op  the map operation from type <code>V</code> to <code>W</code>
     * @return a DistMap from <code>K</code> to <code>W</code> built from applying
     *         the mapping operation on each element of this instance
     */
    public <W> DistMap<K, W> map(Function<V, W> op) {
        throw new Error("not supported yet");
        // TODO
        /*
         * return new DistMap<T,S>(placeGroup, team, () -> { val dst = new
         * HashMap<T,S>(); for (entry in entries()) { val key = entry.getKey(); val
         * value = entry.getValue(); dst(key) = op(value); } return dst; });
         */
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
    public void moveAtSync(Distribution<K> dist, MoveManager mm) {
        final Function<K, Place> rule = (K key) -> {
            return dist.location(key);
        };
        moveAtSync(rule, mm);
    }

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

    /**
     * Parallel version of {@link #forEach(BiConsumer)}
     *
     * @param action the action to perform on every key/value pair contained in this
     *               local map
     */
    public void parallelForEach(SerializableBiConsumer<? super K, ? super V> action) {
        finish(() -> {
            forEachParallelKey(action);
        });
    }

    @Override
    public void parallelForEach(SerializableConsumer<V> action) {
        parallelForEachLocal(action);
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

    private void parallelForEachLocal(SerializableConsumer<V> action) {
        finish(() -> {
            forEachParallelBodyLocal(action);
        });
    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    void printLocalData() {
        System.out.println(this);
    }

    /**
     * Put a new entry.
     *
     * @param key   the key of the new entry.
     * @param value the value of the new entry.
     * @return the previous value associated with {@code key}, or {@code null} if
     *         there was no mapping for {@code key}.(A {@code null} return can also
     *         indicate that the map previously associated {@code null} with
     *         {@code key}.)
     */
    @Override
    public V put(K key, V value) {
        return data.put(key, value);
    }

    /**
     * Adds all the mappings contained in the specified map into this local map.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        data.putAll(m);
    }

    private V putForMove(K key, V value) {
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

    /**
     * Reduce the all local elements using the given operation.
     *
     * @param <S>  type of the result produced by the reduction operation
     * @param op   the operation used in the reduction
     * @param unit the neutral element of the reduction operation
     * @return the result of the reduction
     */
    public <S> S reduceLocal(BiFunction<S, V, S> op, S unit) {
        // TODO may be build-in method for Map
        S accum = unit;
        for (final Map.Entry<K, V> entry : data.entrySet()) {
            accum = op.apply(accum, entry.getValue());
        }
        return accum;
    }

    public void relocate(Distribution<K> rule) throws Exception {
        relocate(rule, new CollectiveMoveManager(placeGroup));
    }

    public void relocate(Distribution<K> rule, CollectiveMoveManager mm) throws Exception {
        for (final K key : data.keySet()) {
            final Place place = rule.location(key);
            // TODO
            // if(place==null) throw SomeException();
            moveAtSync(key, place, mm);
        }
        mm.sync();
    }

    public void relocate(Function<K, Place> rule) throws Exception {
        relocate(rule, new CollectiveMoveManager(placeGroup));
    }

    public void relocate(Function<K, Place> rule, CollectiveMoveManager mm) throws Exception {
        for (final K key : data.keySet()) {
            final Place place = rule.apply(key);
            moveAtSync(key, place, mm);
        }
        mm.sync();
    }

    /**
     * Remove the entry corresponding to the specified key in the local map.
     *
     * @param key the key corresponding to the value.
     * @return the previous value associated with the key, or {@code null} if there
     *         was no existing mapping (or the key was mapped to {@code null})
     */
    @Override
    public V remove(Object key) {
        return data.remove(key);
    }

    private List<Collection<V>> separateLocalValues(int n) {
        final List<Collection<V>> result = new ArrayList<>(n);
        final long totalNum = size();
        final long rem = totalNum % n;
        final long quo = totalNum / n;
        if (data.isEmpty()) {
            return result;
        }
        final Iterator<V> it = data.values().iterator();
        List<V> list = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            list = new ArrayList<>();
            final long count = quo + ((i < rem) ? 1 : 0);
            for (long j = 0; j < count; j++) {
                if (it.hasNext()) {
                    list.add(it.next());
                }
            }
            result.add(list);
        }
        return result;
    }

    /**
     * Sets the proxy generator for this instance.
     * <p>
     * The proxy will be used to generate values when accesses to a key not
     * contained in this instance is made. Instead of throwing an exception, the
     * proxy will be called with the attempted index and the program will continue
     * with the value returned by the proxy.
     * <p>
     * This feature is similar to {@link Map#getOrDefault(Object, Object)}
     * operation, the difference being that instead of returning a predetermined
     * default value, the provided function is called with the key.
     *
     * @param proxy function which takes a key "K" as parameter and returns a "V",
     *              or {@code null} to remove any previously set proxy
     */
    public void setProxyGenerator(Function<K, V> proxy) {
        proxyGenerator = proxy;
    }

    @Override
    public <S extends DistCollectionSatellite<DistMap<K, V>, S>> void setSatellite(S s) {
        satellite = s;
    }

    /**
     * Return the number of local entries.
     *
     * @return the number of the local entries.
     */
    @Override
    public int size() {
        return data.size();
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

    /**
     * Returns all the values of this local map in a collection.
     */
    @Override
    public Collection<V> values() {
        return data.values();
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
