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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;
import mpjbuf.IllegalArgumentException;

/**
 * A Map data structure spread over the multiple places. This class allows
 * multiple values for one key, those values being stored in a list.
 *
 * @param <K> type of the key used in the {@link DistMultiMap}
 * @param <V> type of the elements contained in the lists to which the keys map
 */
public class DistMultiMap<K, V> extends DistMap<K, Collection<V>> {

    /**
     * Construct a DistMultiMap.
     */
    public DistMultiMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistMultiMap with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistMultiMap(TeamedPlaceGroup placeGroup) {
        super(placeGroup);
    }

    /**
     * Construct a DistMultiMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     */
    public DistMultiMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        this(placeGroup, id, new HashMap<>());
    }

    /**
     * Construct a DistMultiMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     * @param data       the container to be used
     */
    protected DistMultiMap(TeamedPlaceGroup placeGroup, GlobalID id, Map<K, Collection<V>> data) {
        super(placeGroup, id, data);
        super.GLOBAL = new GlobalOperations<>(this,
                (TeamedPlaceGroup pg0, GlobalID gid) -> new DistMultiMap<>(pg0, gid));
    }

    /**
     * create an empty collection to hold values for a key. Please define the
     * adequate container for the class.
     *
     * @return the created collection.
     */
    protected Collection<V> createEmptyCollection() {
        return new ArrayList<>();
    }

    // TODO ...
    // public void setupBranches(DistMap.Generator<T,List<U>> gen)

    /**
     * Apply the same operation onto all the local entries.
     *
     * @param op the operation.
     */
    public void forEach1(BiConsumer<K, V> op) {
        for (final Entry<K, Collection<V>> entry : data.entrySet()) {
            final K key = entry.getKey();
            for (final V value : entry.getValue()) {
                op.accept(key, value);
            }
        }
    }

    /**
     * Return {@link MultiMapEntryDispatcher} instance that enable fast relocation
     * between places than normal. One {@link DistMultiMap} has one dispatcher.
     *
     * @param rule Determines the dispatch destination.
     * @return :
     */
    @Override
    public MultiMapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule) {
        return new MultiMapEntryDispatcher<>(this, rule);
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
    @Override
    public MultiMapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule, TeamedPlaceGroup pg)
            throws IllegalArgumentException {
        return new MultiMapEntryDispatcher<>(this, pg, rule);
    }

    /**
     * Apply the same operation on each element including remote places and creates
     * a new {@link DistMultiMap} with the same keys as this instance and the result
     * of the mapping operation as values.
     *
     * @param <W> the type of the result of the map operation
     * @param op  the mapping operation from {@code V} to {@code W}
     * @return a new DistMultiMap which consists of the result of the operation.
     */
    public <W> DistMultiMap<K, W> map(BiFunction<K, V, W> op) {
        // TODO
        throw new Error("not implemented yet");
        /*
         * return new DistMultiMap[T,S](placeGroup, team, () => { val dst = new
         * HashMap[T,List[S]](); for (entry in data.entries()) { val key =
         * entry.getKey(); val old = entry.getValue(); val list = new
         * ArrayList[S](old.size()); for (v in old) { list.add(op(key, v)); }
         * dst(entry.getKey()) = list; } return dst; });
         */
    }

    @Override
    @SuppressWarnings("unchecked")
    public void moveAtSync(Collection<K> keys, Place pl, MoveManager mm) {
        if (pl.equals(here())) {
            return;
        }
        final DistMultiMap<K, V> collection = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final int size = keys.size();
            s.writeInt(size);
            for (final K key : keys) {
                final Collection<V> value = collection.remove(key);
                s.writeObject(key);
                s.writeObject(value);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int size = ds.readInt();
            for (int i = 1; i <= size; i++) {
                final K key = (K) ds.readObject();
                final Collection<V> value = (Collection<V>) ds.readObject();
                collection.putForMove(key, value);
            }
        };
        mm.request(pl, serialize, deserialize);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void moveAtSync(K key, Place pl, MoveManager mm) {
        if (pl.equals(here())) {
            return;
        }
        if (!containsKey(key)) {
            throw new RuntimeException("DistMultiMap cannot move uncontained entry: " + key);
        }
        final DistMultiMap<K, V> toBranch = this; // using plh@AbstractCol
        final Serializer serialize = (ObjectOutput s) -> {
            final Collection<V> value = this.removeForMove(key);
            // TODO we should check values!=null before transportation
            s.writeObject(key);
            s.writeObject(value);
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final K k = (K) ds.readObject();
            // TODO we should check values!=null before transportation
            final Collection<V> v = (Collection<V>) ds.readObject();
            toBranch.putForMove(k, v);
        };
        mm.request(pl, serialize, deserialize);
    }

    /**
     * Puts a new value to the list of specified entry.
     *
     * @param key   the key of the entry
     * @param value the new value to be added to the mappings of {@code key}.
     * @return {@code true} as the collection is modified as a result (as specified
     *         by {@link Collection#add(Object)}.
     */
    public boolean put1(K key, V value) {
        Collection<V> list = data.get(key);
        if (list == null) {
            list = createEmptyCollection();
            data.put(key, list);
        }
        return list.add(value);
    }

    /**
     * Request that the specified value be put in the list of the given key on the
     * specified place when the method {@link CollectiveMoveManager#sync()} of the
     * specified {@link CollectiveMoveManager} instance is called.
     *
     * @param key   the key of the list.
     * @param value the value to be added to the mapping of {@code key}
     * @param pl    the destination place
     * @param mm    MoveManagerLocal handling the data transfers
     */
    @SuppressWarnings("unchecked")
    public void putAtSync(K key, V value, Place pl, CollectiveMoveManager mm) {
        final DistMultiMap<K, V> toBranch = this; // using plh@AbstractCol
        final Serializer serialize = (ObjectOutput s) -> {
            s.writeObject(key);
            s.writeObject(value);
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final K k = (K) ds.readObject();
            final V v = (V) ds.readObject();
            toBranch.put1(k, v);
        };
        mm.request(pl, serialize, deserialize);
    }

    public boolean putForMove(K key, Collection<V> values) {
        Collection<V> list = data.get(key);
        if (list == null) {
            list = createEmptyCollection();
            data.put(key, list);
        }
        // TODO we should check values!=null before transportation
        if (values != null) {
            list.addAll(values);
        }
        return false;
    }

    /**
     * Removes the entry corresponding to the specified key.
     *
     * @param key the key whose mapping need to be removed from this instance
     * @return the list of all the mappings to the specified key.
     */
    public Collection<V> removeForMove(K key) {
        final Collection<V> list = data.remove(key);
        return list;
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistMultiMap<>(pg1, id1);
        });
    }

    /**
     * Reduce the all the local elements using given function.
     *
     * @param op   the operation.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    /*
     * public def reduceLocal[S](op: (S,U)=>S, unit: S): S { var accum: S = unit;
     * for (entry in data.entries()) { for (value in entry.getValue()) { accum =
     * op(accum, value); } } return accum; }
     *
     * def create(placeGroup: PlaceGroup, team: TeamOperations, init: ()=>Map[T,
     * List[U]]){ // return new DistMultiMap[T,U](placeGroup, init) as
     * AbstractDistCollection[Map[T,List[U]]]; return null as
     * AbstractDistCollection[Map[T, List[U]]]; }
     *
     * public def versioningMapList(srcName : String){ // return new
     * BranchingManager[DistMultiMap[T,U], Map[T,List[U]]](srcName, this); return
     * null as BranchingManager[DistMultiMap[T,U], Map[T,List[U]]]; }
     */
    // TODO
    // In the cunnrent implementation of balance(),
    // DistIdMap treat the number of key as the load of the PE, not using the number
    // of elements in the value lists.
}
