/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

/**
 * A Map data structure spread over the multiple places.
 * This class allows multiple values for one key, those values being stored in
 * a list.
 * @param <K> type of the key used in the {@link DistMultiMap}
 * @param <V> type of the elements contained in the lists to which the keys map
 */
public class DistMultiMap<K,V> extends DistMap<K, List<V>> {


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
	 * @param id the global ID used to identify this instance
	 */
	public DistMultiMap(TeamedPlaceGroup placeGroup, GlobalID id) {
		super(placeGroup, id);
	}

	/**
	 * Apply the same operation onto all the local entries.
	 *
	 * @param op the operation.
	 */
	public void forEach1(BiConsumer<K, V> op) {
		for (Map.Entry<K, List<V>> entry: data.entrySet()) {
			K key = entry.getKey();
			for (V value: entry.getValue()) {
				op.accept(key, value);
			}
		}
	}

	// TODO ...
	//public void setupBranches(DistMap.Generator<T,List<U>> gen)

	/**
	 * Apply the same operation on each element including remote places and 
	 * creates a new {@link DistMultiMap} with the same keys as this instance and
	 * the result of the mapping operation as values.
	 * 
	 * @param <W> the type of the result of the map operation
	 * @param op the mapping operation from {@code V} to {@code W}
	 * @return a new DistMultiMap which consists of the result of the operation.
	 */
	public <W> DistMultiMap<K,W> map(BiFunction<K, V, W> op) {
		// TODO
		throw new Error("not implemented yet");
		/*return new DistMultiMap[T,S](placeGroup, team, () => {
            val dst = new HashMap[T,List[S]]();
            for (entry in data.entries()) {
                val key = entry.getKey();
                val old = entry.getValue();
                val list = new ArrayList[S](old.size());
                for (v in old) {
                    list.add(op(key, v));
                }
                dst(entry.getKey()) = list;
            }
            return dst;
        });*/
	}

	@SuppressWarnings("unchecked")
	public void moveAtSync(K key, Place pl, MoveManagerLocal mm) {
		if (pl.equals(here()))
			return;
		if (!containsKey(key))
			throw new RuntimeException("DistMultiMap cannot move uncontained entry: " + key);
		final DistMultiMap<K, V> toBranch = this; // using plh@AbstractCol
		Serializer serialize = (ObjectOutputStream s) -> {
			List<V> value = this.removeForMove(key);
			// TODO we should check values!=null before transportation
			s.writeObject(key);
			s.writeObject(value);
		};
		DeSerializer deserialize = (ObjectInputStream ds) -> {
			K k = (K) ds.readObject();
			// TODO we should check values!=null before transportation
			List<V> v = (List<V>) ds.readObject();
			toBranch.putForMove(k, v);
		};
		mm.request(pl, serialize, deserialize);
	}

	/**
	 * Puts a new value to the list of specified entry.
	 *
	 * @param key the key of the entry
	 * @param value the new value to be added to the mappings of {@code key}.
	 * @return {@code true} as the collection is modified as a result (as 
	 * 	specified by {@link Collection#add(Object)}. 
	 */
	public boolean put1(K key, V value) {
		List<V> list = data.get(key);
		if (list == null) {
			list = new ArrayList<V>();
			data.put(key, list);
		}
		return list.add(value);
	}

	/**
	 * Request that the specified value be put in the list of the given key on 
	 * the specified place when the method {@link MoveManagerLocal#sync()} of 
	 * the specified {@link MoveManagerLocal} instance is called.
	 *
	 * @param key the key of the list.
	 * @param value the value to be added to the mapping of {@code key}
	 * @param pl the destination place
	 * @param mm MoveManagerLocal handling the data transfers
	 */
	@SuppressWarnings("unchecked")
	public void putAtSync(K key, V value, Place pl, MoveManagerLocal mm) {
		DistMultiMap<K,V> toBranch = this; // using plh@AbstractCol
		Serializer serialize = (ObjectOutputStream s) -> {
			s.writeObject(key);
			s.writeObject(value);
		};
		DeSerializer deserialize = (ObjectInputStream ds) -> {
			K k = (K)ds.readObject();
			V v = (V)ds.readObject();
			toBranch.put1(k, v);
		};
		mm.request(pl, serialize, deserialize);
	}

	public boolean putForMove(K key, Collection<V> values) {
		List<V> list = data.get(key);
		if (list == null) {
			list = new ArrayList<V>();
			data.put(key, list);
		}
		// TODO we should check values!=null before transportation
		if (values != null)
			list.addAll(values);
		return false;
	}

	/**
	 * Removes the entry corresponding to the specified key.
	 *
	 * @param key the key whose mapping need to be removed from this instance
	 * @return the list of all the mappings to the specified key. 
	 */
	public List<V> removeForMove(K key) {
		List<V> list = data.remove(key);
		return list;
	}

	public Object writeReplace() throws ObjectStreamException {
		final TeamedPlaceGroup pg1 = placeGroup;
		final GlobalID id1 = id;
		return new LazyObjectReference<DistMultiMap<K,V>>(pg1, id1, ()-> {
			return new DistMultiMap<K,V>(pg1, id1);
		});
	}

	/**
	 * Reduce the all the local elements using given function.
	 *
	 * @param op the operation.
	 * @param unit the zero value of the reduction.
	 * @return the result of the reduction.
	 */
	/*
    public def reduceLocal[S](op: (S,U)=>S, unit: S): S {
        var accum: S = unit;
        for (entry in data.entries()) {
            for (value in entry.getValue()) {
                accum = op(accum, value);
            }
        }
        return accum;
    }

    def create(placeGroup: PlaceGroup, team: TeamOperations, init: ()=>Map[T, List[U]]){
        // return new DistMultiMap[T,U](placeGroup, init) as AbstractDistCollection[Map[T,List[U]]];
        return null as AbstractDistCollection[Map[T, List[U]]];
    }

    public def versioningMapList(srcName : String){
        // return new BranchingManager[DistMultiMap[T,U], Map[T,List[U]]](srcName, this);
        return null as BranchingManager[DistMultiMap[T,U], Map[T,List[U]]];
    }*/
	//TODO
	//In the cunnrent implementation of balance(), 
	// DistIdMap treat the number of key as the load of the PE, not using the number of elements in the value lists. 
}
