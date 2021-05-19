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
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.io.Output;

import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

/**
 * A Map data structure spread over multiple places. This class is used for only
 * add and relocate key-value data over multiple places and will take faster to
 * relocate than {@link DistMap}. <br>
 * <br>
 * The relocation rule is defined by constructor or
 * {@link RelocationMap#setDistribution}. Relocate entries by
 * {@link RelocationMap#relocate}. If you want to use entries, call
 * {@link RelocationMap#convertToDistMap} to get DistMap. Once converted, that
 * RelocationMap instance isn't available. <br>
 * <br>
 * When the same object is added to this Map class multiple times, they will be
 * determined as different objects after relocation.
 *
 * @param <K> type of the key used in the {@link RelocationMap}
 * @param <V> type of the value mapped to each key in the {@link RelocationMap}
 * @author Yoshiki Kawanishi
 */
public class RelocationMap<K, V> implements SerializableWithReplace {

    /** The entries are relocated following the rules defined by this. */
    protected Distribution<K> distribution;

    protected final Map<Place, Map<Thread, ObjectOutput>> outputMap;

    protected DistMap<K, V> distMap;
    public final GlobalID id;
    public final TeamedPlaceGroup placeGroup;

    /**
     * Construct a RelocationMap which can have the rule of relocation.
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     */
    public RelocationMap(Distribution<K> dist) {
        this(dist, TeamedPlaceGroup.getWorld(), new GlobalID(), new DistConcurrentMap<>(TeamedPlaceGroup.getWorld()));
    }

    /**
     * Construct a RelocationMap which can have the rule of relocation and
     * {@link DistConcurrentMap}. Data added to RelocationMap is also add to base
     * {@link DistConcurrentMap}
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     *
     * @param base Data added to this RelocationMap is also add to base
     *             {@link DistConcurrentMap}
     */
    public RelocationMap(Distribution<K> dist, DistConcurrentMap<K, V> base) {
        this(dist, base.placeGroup(), new GlobalID(), base);
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
        this(dist, pg, new GlobalID(), new DistConcurrentMap<>(pg));
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
     * @param dist    the relocation rule class {@link Distribution} decides the
     *                destination of data by key.
     * @param pg      the group of hosts that are susceptible to manipulate this
     *                {@link RelocationMap}
     * @param id      the global id associated to this distributed map.
     *
     * @param distMap TODO
     */
    RelocationMap(Distribution<K> dist, TeamedPlaceGroup pg, GlobalID globalId, DistMap<K, V> distMap) {
        distribution = dist;
        placeGroup = pg;
        id = globalId;
        this.distMap = distMap;
        outputMap = new HashMap<>(placeGroup.size());
        for (final Place pl : placeGroup.places()) {
            final int nThreads = Runtime.getRuntime().availableProcessors();
            outputMap.put(pl, new ConcurrentHashMap<>(nThreads));
        }
    }

    /**
     * Remove the all local entries.
     */
    public void clear() {
        if (distMap == null) {
            throw new IllegalStateException(
                    "RelocationMap#clear is not available after RelocationMap#convertToDistMap is called");
        }
        clearOutputs();
        distMap.clear();
    }

    protected void clearOutputs() {
        for (final Place place : outputMap.keySet()) {
            for (final ObjectOutput output : outputMap.get(place).values()) {
                output.clear();
            }
        }
    }

    /**
     * Returns the {@link DistMap} with the added data. However, the data does not
     * include elements for which the {@link RelocationMap#relocate} method has not
     * been called after the addition.
     */
    public DistMap<K, V> convertToDistMap() {
        final DistMap<K, V> ret = distMap;
        distMap = null;
        clearOutputs();
        return ret;
    }

    public void destroy() {
        placeGroup.remove(id);
        if (distMap != null) {
            placeGroup.remove(distMap.id());
        }
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
     * Put a new entry. The entries before relocation do not exist in a local
     * collection. The entries are relocated when #relocate is called.
     *
     * @param key   the key of the new entry.
     * @param value the value of the new entry.
     * @return If the destination is associated with the key is needless to
     *         relocate, return the previous value associated with {@code key}. If
     *         not, return null.
     */
    public V put(K key, V value) {
        if (distMap == null) {
            throw new IllegalStateException(
                    "RelocationMap#put is not available after RelocationMap#convertToDistMap is called");
        }
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
    public void putAll(Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    private V putLocal(K key, V value) {
        if (distMap.containsKey(key)) {
            throw new IllegalStateException("RelocationMap cannot override existing entry: " + key);
        }
        return distMap.put(key, value);
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
     * Set the rule that defines destination in relocation for entries. The
     * distribution of entries that have already been added is not changed.
     */
    public void setDistribution(Distribution<K> dist) {
        distribution = dist;
    }

    @Deprecated
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        final Distribution<K> distribution1 = distribution;
        final DistConcurrentMap<K, V> distMap1 = (DistConcurrentMap<K, V>) distMap;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new RelocationMap<>(distribution1, pg1, id1, distMap1);
        });
    }
}
