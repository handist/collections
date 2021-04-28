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
import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

import com.esotericsoftware.kryo.io.Output;

import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

/**
 * A Map data structure spread over multiple places. This class is used for only
 * relocate key-value data over multiple places and will take faster to relocate
 * than {@link DistMap}. The added keys and values can't be used until
 * relocated. When the same object is added to this Map class multiple times,
 * they will be determined as different objects after relocation.
 *
 * @param <K> type of the key used in the {@link RelocationMap}
 * @param <V> type of the value mapped to each key in the {@link RelocationMap}
 * @author Yoshiki Kawanishi
 */
public class RelocationMap<K, V> implements Map<K, V>, SerializableWithReplace {

    /** The entries are relocated following the rules defined by this. */
    protected Distribution<K> distribution;

    protected final Map<Place, Map<Thread, ObjectOutput>> outputMap;
    protected Map<K, V> data;
    final GlobalID id;
    public final TeamedPlaceGroup placeGroup;

    /**
     * Construct a RelocationMap which can have the rule of relocation.
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     */
    public RelocationMap(Distribution<K> dist) {
        this(dist, TeamedPlaceGroup.getWorld(), new GlobalID());
    }

    /**
     * Construct a RelocationMap which can have the rule of relocation and local
     * handles on the hosts of the specified {@link TeamedPlaceGroup}
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     * @param pg   the group of hosts that are susceptible to manipulate this
     *             {@link RelocationMap}
     */
    public RelocationMap(Distribution<K> dist, TeamedPlaceGroup pg) {
        this(dist, pg, new GlobalID());
    }

    /**
     * Construct a RelocationMap which can have the rule of relocation and local
     * handles on the hosts of the specified {@link TeamedPlaceGroup}.
     *
     * Specifying a GLobalId which already has object handles registered in other
     * places (potentially objects different from a {@link RelocationMap} instance)
     * could prove disastrous. Instead, programmers should only call
     * {@link #RelocationMap(Distribution)} to create a distributed map with handles
     * on all hosts, or {@link #RelocationMap(Distribution TeamedPlaceGroup)} to
     * restrict their DistMap to a subset of host
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of data by key.
     * @param pg   the group of hosts that are susceptible to manipulate this
     *             {@link RelocationMap}
     * @param id   the global id associated to this distributed map.
     */
    RelocationMap(Distribution<K> dist, TeamedPlaceGroup pg, GlobalID id) {
        distribution = dist;
        placeGroup = pg;
        this.id = id;
        data = new ConcurrentHashMap<>();
        outputMap = new HashMap<>(placeGroup.size());
        for (final Place pl : placeGroup.places()) {
            final int nThreads = Runtime.getRuntime().availableProcessors();
            outputMap.put(pl, new ConcurrentHashMap<>(nThreads));
        }
    }

    /**
     * Remove the all local entries.
     */
    @Override
    public void clear() {
        clearOutputs();
        data.clear();
    }

    private void clearOutputs() {
        for (final Place place : outputMap.keySet()) {
            for (final ObjectOutput output : outputMap.get(place).values()) {
                output.clear();
            }
        }
    }

    /**
     * Return true if the specified entry is exist in the local collection. The
     * entries before relocation do not exist in a local collection.
     *
     * @param key a key.
     * @return true is the specified object is a key present in the local map,
     */
    @Override
    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    /**
     * Return true if the specified entry is exist in the local collection. The
     * entries before relocation do not exist in a local collection.
     *
     * @param value a value.
     * @return true is the specified object is a key present in the local map,
     */
    @Override
    public boolean containsValue(Object value) {
        return data.containsValue(value);
    }

    public void destroy() {
        placeGroup.remove(id);
    }

    /**
     * Return the Set of local entries.The entries before relocation do not exist in
     * a local collection.
     *
     * @return the Set of local entries.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return data.entrySet();
    }

    @SuppressWarnings("unchecked")
    protected void executeDeserialize(byte[] buf, int[] offsets, int[] sizes) throws Exception {
        int current = 0;
        for (final Place p : placeGroup.places()) {
            final int size = sizes[current];
            final int offset = offsets[current];
            current++;
            if (p.equals(here()) || size == 0) {
                continue;
            }
            final ObjectInput ds = new ObjectInput(new ByteArrayInputStream(buf, offset, size), false);
            final int nThreads = ds.readInt();
            for (int i = 0; i < nThreads; i++) {
                final int count = ds.readInt();
                for (int j = 0; j < count; j += 2) { // NOTE: convert key and value(two object), so "J+=2".
                    final K key = (K) ds.readObject();
                    final V value = (V) ds.readObject();
                    putLocal(key, value);
                }
                ds.reset();
            }
            ds.close();
        }
    }

    private void executeSummerizeOutput(ByteArrayOutputStream out, int[] offsets, int[] sizes) throws IOException {
        for (int i = 0; i < placeGroup.size(); i++) {
            final Place place = placeGroup.get(i);
            if (place.equals(here())) {
                continue;
            }
            offsets[i] = out.size();
            Output output = new Output(out);
            output.writeInt(outputMap.get(place).size());
            output.close();
            for (final ObjectOutput o : outputMap.get(place).values()) {
                output = new Output(out);
                output.writeInt(o.getCount());
                output.close();
                out.write(o.toByteArray());
            }
            out.flush();
            sizes[i] = out.size() - offsets[i];
        }
    }

    /**
     * Apply the specified operation with each Key/Value pair contained in the local
     * collection. The entries before relocation do not exist in a local collection.
     *
     * @param action the operation to perform
     */
    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        if (!data.isEmpty()) {
            data.forEach(action);
        }
    }

    /**
     * Return the element for the provided key. If there is no element at the index,
     * return null. The entries before relocation do not exist in a local
     * collection.
     *
     * @param key the index of the value to retrieve
     * @return the element associated with {@code key}.
     */
    @Override
    public V get(Object key) {
        return data.get(key);
    }

    protected ObjectOutput getOutput(Place place) {
        final Thread thread = Thread.currentThread();
        ObjectOutput out = outputMap.get(place).get(thread);
        if (out == null) {
            out = new ObjectOutput(new ByteArrayOutputStream(), false);
            outputMap.get(place).put(thread, out);
        }
        return out;
    }

    /**
     * Indicates if the local distributed map is empty or not. The entries before
     * relocation do not exist in a local collection.
     *
     * @return {@code true} if there are no mappings in the local map
     */
    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Return the Set of local keys. The entries before relocation do not exist in a
     * local collection.
     *
     * @return the Set of local keys.
     */
    @Override
    public Set<K> keySet() {
        return data.keySet();
    }

    /**
     * Put a new entry. The entries before relocation do not exist in a local
     * collection. The entries are relocated when #relocate is called.
     *
     * @param key   the key of the new entry.
     * @param value the value of the new entry.
     * @return If the destination is associated with the key is needless to
     *         relocate, return the previous value associated with {@code key}. If
     *         not, return null.
     */
    @Override
    public V put(K key, V value) {
        final Place next = distribution.place(key);
        if (next.equals(here())) {
            return putLocal(key, value);
        }
        final ObjectOutput out = getOutput(next);
        out.writeObject(key);
        out.writeObject(value);
        out.flush();
        return null;
    }

    /**
     * Adds all the mappings contained in the specified map. They do not exist in a
     * local collection. They are relocated when #relocate is called.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    private V putLocal(K key, V value) {
        if (data.containsKey(key)) {
            throw new RuntimeException("RelocationMap cannot override existing entry: " + key);
        }
        return data.put(key, value);
    }

    /**
     * Relocate the entries that was put. Destination is defined by
     * {@link Distribution}.
     */
    public void relocate() throws Exception {
        final int[] sendOffset = new int[placeGroup.size()];
        final int[] sendSize = new int[placeGroup.size()];
        final int[] rcvOffset = new int[placeGroup.size()];
        final int[] rcvSize = new int[placeGroup.size()];
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        executeSummerizeOutput(out, sendOffset, sendSize);
        final byte[] buf = CollectiveRelocator.exchangeBytesWithinGroup(placeGroup, out.toByteArray(), sendOffset,
                sendSize, rcvOffset, rcvSize);
        executeDeserialize(buf, rcvOffset, rcvSize);
        clearOutputs();
    }

    /**
     * Thie method is called like {@link GlobalOperations}. Relocate the entries
     * that was put. Destination is defined by {@link Distribution}.
     */
    public void relocateGlobal() {
        placeGroup.broadcastFlat(() -> {
            this.relocate();
        });
    }

    /**
     * Remove the entry corresponding to the specified key in the local map. The
     * entries before relocation do not exist in a local collection.
     *
     * @param key the key corresponding to the value.
     * @return the previous value associated with the key, or {@code null} if there
     *         was no existing mapping (or the key was mapped to {@code null})
     */
    @Override
    public V remove(Object key) {
        return data.remove(key);
    }

    /**
     * Set the rule that defines destination in relocation for entries. The
     * distribution of entries that have already been added is not changed.
     */
    public void setDistribution(Distribution<K> dist) {
        distribution = dist;
    }

    /**
     * Return the number of the local entries. The entries before relocation is not
     * exist local collection.
     *
     * @return the number of the local entries.
     */
    @Override
    public int size() {
        return data.size();
    }

    /**
     * Returns all the values of this local map in a collection. The entries before
     * relocation do not exist in a local collection.
     */
    @Override
    public Collection<V> values() {
        return data.values();
    }

    @Deprecated
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        final Distribution<K> dist = distribution;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new RelocationMap<>(dist, pg1, id1);
        });
    }
}
