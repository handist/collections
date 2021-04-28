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
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;

/**
 * A Map data structure spread over multiple places. This class is used for only
 * relocate key-value data over multiple places and will take faster to relocate
 * than {@link DistMultiMap}. This class allows multiple values for one key,
 * those values being stored in a list. The added keys and values can't be used
 * until relocated. When the same object is added to this Map class multiple
 * times, they will be determined as different objects after relocation.
 *
 * @param <K> type of the key used in the {@link RelocationMultiMap}
 * @param <V> type of the value mapped to each key in the
 *            {@link RelocationMultiMap}
 * @author Yoshiki Kawanishi
 */
public class RelocationMultiMap<K, V> extends RelocationMap<K, List<V>> {

    @SuppressWarnings("unchecked")
    protected final DeSerializer deserializer = (ObjectInput in) -> {
        final K key = (K) in.readObject();
        final V value = (V) in.readObject();
        this.put1Local(key, value);
    };

    /**
     * Construct a DistMap which can have the rule of relocation.
     *
     * @param dist the relocation rule class {@link Distribution} decides the
     *             destination of added entries by their keys.
     */
    public RelocationMultiMap(Distribution<K> dist) {
        this(dist, TeamedPlaceGroup.getWorld(), new GlobalID());
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
        this(dist, pg, new GlobalID());
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
    public RelocationMultiMap(Distribution<K> dist, TeamedPlaceGroup pg, GlobalID id) {
        super(dist, pg, id);
    }

    /**
     * Apply the specified operation with each Key/Value pair contained in the local
     * collection. The entries before relocation is not exist local collection.
     *
     * @param op action the operation to perform
     */
    public void forEach1(BiConsumer<K, V> op) {
        for (final Map.Entry<K, List<V>> entry : data.entrySet()) {
            final K key = entry.getKey();
            for (final V value : entry.getValue()) {
                op.accept(key, value);
            }
        }
    }

    @Override
    @Deprecated
    public List<V> put(K key, List<V> values) {
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
        List<V> list = data.get(key);
        if (list == null) {
            list = new ArrayList<>();
            data.put(key, list);
        }
        return list.add(value);
    }

    @Override
    @Deprecated
    public void putAll(Map<? extends K, ? extends List<V>> m) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        final Distribution<K> dist = distribution;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new RelocationMultiMap<>(dist, pg1, id1);
        });
    }
}
