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
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

/**
 * A Map data structure spread over multiple places. This class is used for only
 * add and relocate key-value data over multiple places and will take faster to
 * relocate than {@link DistMultiMap}. This class allows multiple values for one
 * key, those values being stored in a list. <br>
 * <br>
 * The relocation rule is defined by constructor or
 * {@link RelocationMap#setDistribution}. Relocate entries by
 * {@link RelocationMap#relocate}. If you want to use entries, call
 * {@link RelocationMultiMap#convertToDistMap} to get DistMap. Once converted,
 * that RelocationMap instance isn't available. <br>
 * <br>
 * When the same object is added to this Map class multiple times, they will be
 * determined as different objects after relocation.
 *
 * @param <K> type of the key used in the {@link RelocationMultiMap}
 * @param <V> type of the value mapped to each key in the
 *            {@link RelocationMultiMap}
 * @author Yoshiki Kawanishi
 */
public class RelocationMultiMap<K, V> extends RelocationMap<K, Collection<V>> {

    /**
     * Construct a RelocationMultiMap which can have the rule of relocation.
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     */
    public RelocationMultiMap(Distribution<K> dist) {
        this(dist, TeamedPlaceGroup.getWorld(), new GlobalID(),
                new DistConcurrentMultiMap<>(TeamedPlaceGroup.getWorld()));
    }

    /**
     * Construct a RelocationMultiMap which can have the rule of relocation and
     * {@link DistConcurrentMultiMap}. Data added to RelocationMap is also add to
     * base {@link DistConcurrentMultiMap}
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     *
     * @param base Data added to this RelocationMap is also add to base
     *             {@link DistConcurrentMap}
     */
    public RelocationMultiMap(Distribution<K> dist, DistConcurrentMultiMap<K, V> base) {
        this(dist, base.placeGroup(), new GlobalID(), base);
    }

    /**
     * Construct a DistMap which can have the rule of relocation and local handles
     * on the hosts of the specified {@link TeamedPlaceGroup}
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     * @param pg   the group of hosts that are susceptible to manipulate this
     *             {@link RelocationMultiMap}
     */
    public RelocationMultiMap(Distribution<K> dist, TeamedPlaceGroup pg) {
        this(dist, pg, new GlobalID(), new DistConcurrentMultiMap<>(pg));
    }

    /**
     * Construct a DistMap which can have the rule of relocation and local handles
     * on the hosts of the specified {@link TeamedPlaceGroup}.
     *
     * Specifying a GLobalId which already has object handles registered in other
     * places (potentially objects different from a {@link RelocationMap} instance)
     * could prove disastrous. Instead, programmers should only call
     * {@link #RelocationMultiMap(Distribution)} to create a distributed map with
     * handles on all hosts, or
     * {@link #RelocationMultiMap(Distribution TeamedPlaceGroup)} to restrict their
     * DistMap to a subset of host
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of data by key.
     * @param pg   the group of hosts that are susceptible to manipulate this
     *             {@link RelocationMultiMap}
     * @param id   the global id associated to this distributed map.
     */
    RelocationMultiMap(Distribution<K> dist, TeamedPlaceGroup pg, GlobalID globalId, DistMultiMap<K, V> distMap) {
        super(dist, pg, globalId, distMap);
    }

    /**
     * Returns the {@link DistMultiMap} with the added data. However, the data does
     * not include elements for which the {@link RelocationMap#relocate} method has
     * not been called after the addition.
     */
    @Override
    public DistConcurrentMultiMap<K, V> convertToDistMap() {
        final DistConcurrentMultiMap<K, V> ret = (DistConcurrentMultiMap<K, V>) distMap;
        distMap = null;
        clearOutputs();
        return ret;
    }

    @Override
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
                    put1Local(key, value);
                }
                ds.reset();
            }
            ds.close();
        }
    }

    /**
     * Unsupported.
     */
    @Override
    @Deprecated
    public Collection<V> put(K key, Collection<V> values) {
        throw new UnsupportedOperationException();
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
    public boolean put1(K key, V value) {
        if (distMap == null) {
            throw new IllegalStateException(
                    "RelocationMultiMap#put1 is not available after RelocationMap#convertToDistMap is called");
        }
        final Place next = distribution.place(key);
        if (next.equals(here())) {
            return put1Local(key, value);
        }
        final ObjectOutput out = getOutput(next);
        out.writeObject(key);
        out.writeObject(value);
        out.flush();
        return false;
    }

    private boolean put1Local(K key, V value) {
        Collection<V> list = distMap.get(key);
        if (list == null) {
            list = new ArrayList<>();
            distMap.put(key, list);
        }
        return list.add(value);
    }

    /**
     * Unsupported.
     */
    @Override
    @Deprecated
    public void putAll(Map<? extends K, ? extends Collection<V>> m) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        final Distribution<K> distribution1 = distribution;
        final DistConcurrentMultiMap<K, V> distMap1 = (DistConcurrentMultiMap<K, V>) distMap;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new RelocationMultiMap<>(distribution1, pg1, id1, distMap1);
        });
    }
}
