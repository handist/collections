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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Function;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.LongRange;
import handist.collections.dist.ElementLocationManager.ParameterErrorException;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.Serializer;

/**
 * Distributed Map using {@link Long} as key and type <code>V</code> as value.
 * <h2>Distribution</h2>
 * <p>
 * This map implementation has its distribution tracked using an internal
 * mechanism. A snapshot of the current distribution can be obtained through
 * method {@link #getDistribution()}.
 *
 * @param <V> the type of the value mappings of this instance
 */
public class DistIdMap<V> extends DistSortedMap<Long, V> implements DistributedCollection<V, DistMap<Long, V>>,
        ElementLocationManageable<Long>, RangeRelocatable<LongRange> {

    private static int _debug_level = 0;
    protected final transient ElementLocationManager<Long> ldist;

    /**
     * Construct a DistIdMap. {@link TeamedPlaceGroup#getWorld()} is used as the
     * PlaceGroup of the new instance, a new {@link GlobalID} will also be created
     * for this new collection.
     */
    public DistIdMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistIdMap with the given argument. TeamOperations(placeGroup) is
     * used as the PlaceGroup of the new instance.
     *
     * @param placeGroup the PlaceGroup.
     */
    public DistIdMap(TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    protected DistIdMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id, new ConcurrentSkipListMap<>());
        super.GLOBAL = new GlobalOperations<>(this, (TeamedPlaceGroup pg0, GlobalID gid) -> new DistIdMap<>(pg0, gid));
        // TODO
        this.ldist = new ElementLocationManager<>();
        ldist.setup(data.keySet());
    }

    /**
     * Remove the all local entries.
     */
    @Override
    public void clear() {
        for (final Long k : data.keySet()) {
            ldist.remove(k);
        }
        super.clear();
        Arrays.fill(locality, 1.0f);
    }

    /*
     * Return true if the entry corresponding to the specified id is local.
     *
     * @return true or false.
     */
    public boolean containsId(long id) {
        return super.containsKey(id);
    }

    public boolean delete(long id) {
        ldist.remove(id);
        return super.delete(id);
    }

    /*
     * Ensure calling updateDist() before balance() balance() should be called in
     * all places
     */
    public void distSize(long[] result) {
        for (final Map.Entry<Long, Place> entry : ldist.dist.entrySet()) {
            // val k = entry.getKey();
            final Place v = entry.getValue();
            result[placeGroup.rank(v)] += 1;
        }
    }

    /**
     * Execute the specified operation with the corresponding value of the specified
     * id.
     * <ul>
     * <li>If the entry is stored at local, the operation is executed sequentially.
     * <li>If the entry is stored at a remote place, the operation is asynchronously
     * executed on the remote place
     * </ul>
     * <p>
     * In the remote case, this method returns immediately. Actual completion of the
     * operation can only be guaranteed if this method's enclosing
     * {@link apgas.Constructs#finish(apgas.Job)} has returned.
     *
     * @param id a Long type value.
     * @param op the operation.
     */
    public void execAt(long id, SerializableConsumer<V> op) {
        final Place place = getPlace(id);
        if (place.equals(here())) {
            op.accept(data.get(id));
            return;
        }
        asyncAt(place, () -> {
            op.accept(data.get(id));
        });
    }

    /**
     * Get the corresponding value of the specified id in the local collection.
     *
     * @param id long index to retrieve
     * @return the corresponding value of the specified index, or {@code null} if
     *         the corresponding mapping was null or if there is no such mapping in
     *         the local collection
     */
    public V get(long id) {
        return data.get(id);
    }

    @Deprecated
    @Override
    public Collection<LongRange> getAllRanges() {
        // Find range from the beginning. Modify if more efficient implementation.
        final Collection<LongRange> ret = new ArrayList<>();
        Long key = firstKey();
        Long from = key;
        final Long last = lastKey();

        while (key <= last) {
            final Long next = higherKey(key);
            if (next == null) {
                ret.add(new LongRange(from, key + 1));
                break;
            }
            if (next != key + 1l) {
                ret.add(new LongRange(from, key + 1));
                from = next;
            }
            key = next;
        }
        return ret;
    }

    Map<Long, Integer> getDiff() {
        return ldist.diff;
    }

    /**
     * Returns a newly created snapshot of the distribution of this
     * {@link DistIdMap}. Subsequent modifications to the distribution of this
     * distributed map will not be reflected into the returned instance.
     * <p>
     * In case an updated {@link LongDistribution} is needed, consider using
     * {@link #registerDistribution(UpdatableDistribution)} to update the
     * {@link LongDistribution} instance each time {@link #updateDist()} is called.
     * This is more efficient than calling {@link #getDistribution()}
     *
     * @return a newly created snapshot of the current distribution of this
     *         collection.
     */
    public LongDistribution getDistribution() {
        return new LongDistribution(ldist.dist);
    }

    /*
     * Get a place where the the corresponding entry of the specified id is stored.
     * Return null when it doesn't exist.
     *
     * @param id a Long type value.
     *
     * @return the Place.
     */
    public Place getPlace(long id) {
        return ldist.dist.get(id);
    }

    @Override
    public void getSizeDistribution(long[] result) {
        for (final Map.Entry<Long, Place> entry : ldist.dist.entrySet()) {
            final Place p = entry.getValue();
            result[placeGroup.rank(p)]++;
        }
    }

    /*
     * Return the Set of local ids.
     *
     * @return the Set of local ids.
     */
    public Set<Long> idSet() {
        return keySet();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void moveAtSync(Collection<Long> keys, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        final DistIdMap<V> collection = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final int size = keys.size();
            s.writeInt(size);
            for (final Long key : keys) {
                final V value = collection.removeForMove(key);
                final byte mType = ldist.moveOut(key, dest);
                s.writeLong(key);
                s.writeByte(mType);
                s.writeObject(value);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int size = ds.readInt();
            for (int i = 0; i < size; i++) {
                final long key = ds.readLong();
                final byte mType = ds.readByte();
                final V value = (V) ds.readObject();
                collection.putForMove(key, mType, value);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    @Override
    public void moveAtSync(Distribution<Long> dist, MoveManager mm) {
        final Function<Long, Place> rule = (Long key) -> {
            return dist.location(key);
        };
        moveAtSync(rule, mm);
    }

    @Override
    public void moveAtSync(Function<Long, Place> rule, MoveManager mm) {
        final DistIdMap<V> collection = this;
        final HashMap<Place, ArrayList<Long>> keysToMove = new HashMap<>();
        collection.forEach((Long key, V value) -> {
            final Place destination = rule.apply(key);
            if (!keysToMove.containsKey(destination)) {
                keysToMove.put(destination, new ArrayList<Long>());
            }
            keysToMove.get(destination).add(key);
        });
        for (final Place p : keysToMove.keySet()) {
            moveAtSync(keysToMove.get(p), p, mm);
        }
    }

    @SuppressWarnings("unchecked")
    public void moveAtSync(final long key, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }

        final DistIdMap<V> toBranch = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final V value = this.removeForMove(key);
            final byte mType = ldist.moveOut(key, dest);
            s.writeLong(key);
            s.writeByte(mType);
            s.writeObject(value);
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final long k = ds.readLong();
            final byte mType = ds.readByte();
            final V v = (V) ds.readObject();
            if (_debug_level > 5) {
                System.err.println("[" + here() + "] putForMove key: " + k + " keyType: " + mType + " value: " + v);
            }
            toBranch.putForMove(k, mType, v);
        };
        mm.request(dest, serialize, deserialize);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void moveAtSync(Long from, Long to, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }

        final DistIdMap<V> toBranch = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final ConcurrentNavigableMap<Long, V> sub = data.subMap(from, to);
            final int num = sub.size();
            s.writeInt(num);
            if (num == 0) {
                return;
            }
            final Iterator<Long> iter = sub.keySet().iterator();
            while (iter.hasNext()) {
                final Long key = iter.next();
                final V value = this.removeForMove(key);
                if (value == null) {
                    throw new NullPointerException("DistIdMap.moveAtSync null pointer value of key: " + key);
                }
                final byte mType = ldist.moveOut(key, dest);
                s.writeLong(key);
                s.writeByte(mType);
                s.writeObject(value);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int num = ds.readInt();
            for (int i = 0; i < num; i++) {
                final long k = ds.readLong();
                final byte mType = ds.readByte();
                final V v = (V) ds.readObject();
                toBranch.putForMove(k, mType, v);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    @Override
    public void moveAtSync(Long key, Place dest, MoveManager mm) {
        moveAtSync(key.longValue(), dest, mm);
    }

    /**
     * Marks keys inner given range of this local handle for relocation using the
     * provided distribution to determine where each individual keys should go. The
     * transfer is actually performed the next the specified manager's
     * {@link CollectiveMoveManager#sync()} method is called.
     *
     * @param range transfer entries which keys index is range.from or more and less
     *              than range.to.
     * @param rule  the function that determines where each individual range should
     *              be relocated to
     * @param mm    the move manager in charge of the transfer
     */
    public void moveAtSync(LongRange range, LongRangeDistribution rule, MoveManager mm) {
        final Map<LongRange, Place> dest = rule.rangeLocation(range);
        for (final Entry<LongRange, Place> entry : dest.entrySet()) {
            moveRangeAtSync(entry.getKey(), entry.getValue(), mm);
        }
    }

    /**
     * Marks all keys of this local handle for relocation using the provided
     * distribution to determine where each individual keys should go. The transfer
     * is actually performed the next the specified manager's
     * {@link CollectiveMoveManager#sync()} method is called.
     *
     * @param rule the function that determines where each individual range should
     *             be relocated to
     * @param mm   the move manager in charge of the transfer
     */
    public void moveAtSync(LongRangeDistribution rule, MoveManager mm) {
        final NavigableMap<Long, V> map = data;
        moveAtSync(new LongRange(map.firstKey(), map.lastKey() + 1), rule, mm);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void moveAtSyncCount(int count, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        final DistIdMap<V> collection = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final int size = count;
            s.writeInt(size);
            final long[] keys = new long[size];
            final Object[] values = new Object[size];

            int i = 0;
            for (final Map.Entry<Long, V> entry : data.entrySet()) {
                if (i == size) {
                    break;
                }
                keys[i] = entry.getKey();
                values[i] = entry.getValue();
                i += 1;
            }
            for (int j = 0; j < size; j++) {
                s.writeLong(keys[j]);
            }
            for (int j = 0; j < size; j++) {
                s.writeObject(values[j]);
            }
            for (int j = 0; j < size; j++) {
                final long key = keys[j];
                collection.removeForMove(key);
                final byte mType = ldist.moveOut(key, dest);
                s.writeByte(mType);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int size = ds.readInt();
            final long[] keys = new long[size];
            final Object[] values = new Object[size];

            for (int j = 0; j < size; j++) {
                keys[j] = ds.readLong();
            }
            for (int j = 0; j < size; j++) {
                values[j] = ds.readObject();
            }
            for (int j = 0; j < size; j++) {
                final byte mType = ds.readByte();
                collection.putForMove(keys[j], mType, (V) values[j]);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    /**
     * Marks keys inner given range for relocation over to the specified place. The
     * actual transfer will be performed the next time the specified manager's
     * {@link CollectiveMoveManager#sync()} method is called.
     *
     * @param range transfer entries which keys index is range.from or more and less
     *              than range.to.
     * @param dest  the place to which these keys should be relocated
     * @param mm    the manager in charge of performing the relocation
     */
    @Override
    public void moveRangeAtSync(LongRange range, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        moveAtSync(range.from, range.to, dest, mm);
    }

    /**
     * Put a new entry into the local map entry
     *
     * @param id    a Long type value.
     * @param value a value.
     */
    public V put(long id, V value) throws Exception {
        if (data.containsKey(id)) {
            return data.put(id, value);
        }
        ldist.add(id);
        return data.put(id, value);
    }

    @Override
    public V put(Long key, V value) {
        if (data.containsKey(key)) {
            return data.put(key, value);
        }
        ldist.add(key);
        return data.put(key, value);
    }

    protected V putForMove(long key, byte mType, V value) throws Exception {
        switch (mType) {
        case ElementLocationManager.MOVE_NEW:
            ldist.moveInNew(key);
            break;
        case ElementLocationManager.MOVE_OLD:
            ldist.moveInOld(key);
            break;
        default:
            throw new Exception("SystemError when calling putForMove " + key);
        }
        return super.putForMove(key, value);
    }

    @Override
    public void registerDistribution(UpdatableDistribution<Long> distributionToUpdate) {
        ldist.registerDistribution(distributionToUpdate);
    }

    /**
     * TODO : move to team operation and global operation
     *
     * @param rule the function that determines where each individual range should
     *             be relocated to
     */
    public void relocate(LongRangeDistribution rule) throws Exception {
        final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup());
        moveAtSync(rule, mm);
        mm.sync();
    }

    /**
     * TODO : move to team operation and global operation
     *
     * @param rule the function that determines where each individual range should
     *             be relocated to
     * @param mm   the move manager in charge of the transfer
     */
    public void relocate(LongRangeDistribution rule, CollectiveMoveManager mm) throws Exception {
        moveAtSync(rule, mm);
        mm.sync();
    }

    /**
     * Removes a mapping from this distributed map local handle, or
     * <code>null</code> if the object supplied as parameter is not a {@link Long}.
     *
     * @param key the Long key
     * @return the value to which the key was previously mapped to
     * @throws ParameterErrorException if there are no mappings associated with the
     *                                 specified key
     */
    @Override
    public V remove(Object key) {
        if (key instanceof Long) {
            ldist.remove((Long) key);
            return super.remove(key);
        } else {
            return null;
        }
    }

    private V removeForMove(long id) {
        return data.remove(id);
    }

    /**
     * Update the distribution information of the entries.
     */
    @Override
    public void updateDist() {
        ldist.update(placeGroup);
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistIdMap<>(pg1, id1);
        });
    }

    /*
     * //TODO different naming convention of balance methods with DistMap public
     * void balance(MoveManagerLocal mm) throws Exception { int pgSize =
     * placeGroup.size(); ArrayList<IntFloatPair> listPlaceLocality = new
     * ArrayList<>(); float localitySum = 0.0f; long globalDataSize =0; long[]
     * localDataSize = new long[pgSize];
     *
     * for (int i = 0; i<locality.length; i++) { localitySum += locality[i]; }
     *
     *
     * for (int i=0; i< pgSize; i++) { globalDataSize += localDataSize[i]; float
     * normalizeLocality = locality[i] / localitySum; listPlaceLocality.add(new
     * IntFloatPair(i, normalizeLocality)); }
     *
     * listPlaceLocality.sort((IntFloatPair a1, IntFloatPair a2)->{ return
     * (a1.second < a2.second) ? -1 : (a1.second > a1.second) ? 1 : 0; });
     *
     * if (_debug_level > 5) { for (IntFloatPair pair: listPlaceLocality) {
     * System.out.print("(" + pair.first + ", " + pair.second + ") "); }
     * System.out.println(); placeGroup.barrier(); // for debug print }
     *
     * IntFloatPair[] cumulativeLocality = new IntFloatPair[pgSize]; float
     * sumLocality = 0.0f; for (int i=0; i<pgSize; i++) { sumLocality +=
     * listPlaceLocality.get(i).second; cumulativeLocality[i] = new
     * IntFloatPair(listPlaceLocality.get(i).first, sumLocality); }
     * cumulativeLocality[pgSize - 1] = new
     * IntFloatPair(listPlaceLocality.get(pgSize - 1).first, 1.0f);
     *
     * if (_debug_level > 5) { for (int i=0; i<pgSize; i++) { IntFloatPair pair =
     * cumulativeLocality[i]; System.out.print("(" + pair.first + ", " + pair.second
     * + ", " + localDataSize[pair.first] + "/" + globalDataSize + ") "); }
     * System.out.println(); placeGroup.barrier(); // for debug print }
     *
     * ArrayList<ArrayList<IntLongPair>> moveList = new ArrayList<>(pgSize); //
     * ArrayList(index of dest Place, num data to export) LinkedList<IntLongPair>
     * stagedData = new LinkedList<>(); // ArrayList(index of src, num data to
     * export) long previousCumuNumData = 0;
     *
     * for (int i=0; i<pgSize; i++) { moveList.add(new ArrayList<IntLongPair>()); }
     *
     * for (int i=0; i<pgSize; i++) { int placeIdx = cumulativeLocality[i].first;
     * float placeLocality = cumulativeLocality[i].second; long cumuNumData = (long)
     * (((float)globalDataSize) * placeLocality); long targetNumData = cumuNumData -
     * previousCumuNumData; if (localDataSize[placeIdx] > targetNumData) {
     * stagedData.add(new IntLongPair(placeIdx, localDataSize[placeIdx] -
     * targetNumData)); if (_debug_level > 5) { System.out.print("stage src: " +
     * placeIdx + " num: " + (localDataSize[placeIdx] - targetNumData) + ", "); } }
     * previousCumuNumData = cumuNumData; } if (_debug_level > 5) {
     * System.out.println(); placeGroup.barrier(); // for debug print }
     *
     * previousCumuNumData = 0; for (int i=0; i<pgSize; i++) { int placeIdx =
     * cumulativeLocality[i].first; float placeLocality =
     * cumulativeLocality[i].second; long cumuNumData =
     * (long)(((float)globalDataSize) * placeLocality); long targetNumData =
     * cumuNumData - previousCumuNumData; if (targetNumData >
     * localDataSize[placeIdx]) { long numToImport = targetNumData -
     * localDataSize[placeIdx]; while (numToImport > 0) { IntLongPair pair =
     * stagedData.removeFirst(); if (pair.second > numToImport) {
     * moveList.get(pair.first).add(new IntLongPair(placeIdx, numToImport));
     * stagedData.add(new IntLongPair(pair.first, pair.second - numToImport));
     * numToImport = 0; } else { moveList.get(pair.first).add(new
     * IntLongPair(placeIdx, pair.second)); numToImport -= pair.second; } } }
     * previousCumuNumData = cumuNumData; }
     *
     * if (_debug_level > 5) { for (int i=0; i<pgSize; i++) { for (IntLongPair pair:
     * moveList.get(i)) { System.out.print("src: " + i + " dest: " + pair.first +
     * " size: " + pair.second + ", "); } } System.out.println();
     * placeGroup.barrier(); // for debug print }
     *
     *
     * if (_debug_level > 5) { long[] diffNumData = new long[pgSize]; for (int i=0;
     * i<pgSize; i++) { for (IntLongPair pair: moveList.get(i)) { diffNumData[i] -=
     * pair.second; diffNumData[pair.first] += pair.second; } } for (IntFloatPair
     * pair: listPlaceLocality) { System.out.print("(" + pair.first + ", " +
     * pair.second + ", " + (localDataSize[pair.first] + diffNumData[pair.first]) +
     * "/" + globalDataSize + ") "); } System.out.println(); placeGroup.barrier();
     * // for debug print }
     *
     * // Move Data for (int i=0; i<pgSize; i++) { if
     * (placeGroup.get(i).equals(here())) {
     *
     * } }
     *
     * }
     *
     * public void balance(float[] newLocality, MoveManagerLocal mm) throws
     * Exception { System.arraycopy(newLocality, 0, locality, 0, placeGroup().size);
     * balance(mm); }
     */
}
