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
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import apgas.Place;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

/**
 * {@link DistMap} has this class in order to dispatch entris to places defined
 * by {@link Distribution}. Relocate entries between places by calling
 * {@link MapEntryDispatcher.TeamOperations#dispatch}. Dhe dispatched entries
 * are added to original {@link DistMap}.
 *
 * Please be careful that reference relationships like multiple references to
 * one object are not maintained.
 *
 * @author yoshikikawanishi
 *
 * @param <K> : The key type of map entry
 * @param <V> : The value type of map entry
 */
public class MapEntryDispatcher<K, V> implements KryoSerializable, Serializable {

    class TeamOperations {
        private final MapEntryDispatcher<K, V> handle;

        public TeamOperations(MapEntryDispatcher<K, V> localObject) {
            handle = localObject;
        }

        /**
         * Relocate the entries that was put. Destination is defined by
         * {@link Distribution}.
         */
        public void dispatch() throws Exception {
            final int[] sendOffset = new int[placeGroup.size()];
            final int[] sendSize = new int[placeGroup.size()];
            final int[] rcvOffset = new int[placeGroup.size()];
            final int[] rcvSize = new int[placeGroup.size()];
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            handle.executeSummerizeOutput(out, sendOffset, sendSize);
            final byte[] buf = CollectiveRelocator.exchangeBytesWithinGroup(placeGroup, out.toByteArray(), sendOffset,
                    sendSize, rcvOffset, rcvSize);
            handle.executeDeserialize(buf, rcvOffset, rcvSize);
            clearOutputs();
        }
    }

    private static final long serialVersionUID = 6002859058320904125L;

    protected TeamedPlaceGroup placeGroup;
    protected DistMap<K, V> base;
    protected TeamOperations TEAM;

    protected Map<Place, Map<Thread, ObjectOutput>> outputMap;
    /** The entries are relocated following the rules defined by this. */
    protected Distribution<K> distribution;

    /**
     * @param distMap
     * @param dist
     */
    MapEntryDispatcher(DistMap<K, V> distMap, Distribution<K> dist) {
        this(distMap, distMap.placeGroup(), dist);
    }

    /**
     * @param distMap
     * @param pg
     * @param dist
     */
    MapEntryDispatcher(DistMap<K, V> distMap, TeamedPlaceGroup pg, Distribution<K> dist) {
        base = distMap;
        placeGroup = pg;
        TEAM = new TeamOperations(this);
        outputMap = new HashMap<>(placeGroup.size());
        for (final Place pl : placeGroup.places()) {
            final int nThreads = Runtime.getRuntime().availableProcessors();
            outputMap.put(pl, new ConcurrentHashMap<>(nThreads));
        }
        distribution = dist;
    }

    /**
     * Remove the all local entries.
     */
    public void clear() {
        clearOutputs();
        base.clear();
    }

    protected void clearOutputs() {
        for (final Place place : outputMap.keySet()) {
            for (final ObjectOutput output : outputMap.get(place).values()) {
                output.clear();
            }
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
     * Put a new entry. The entries are relocated when #relocate is called.
     *
     * @param key   the key of the new entry.
     * @param value the value of the new entry.
     * @return If the destination associated with the key is here, return the value.
     *         If not, return null.
     */
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
     * Adds all the mappings contained in the specified map.
     */
    public void putAll(Map<? extends K, ? extends V> m) {
        for (final Entry<? extends K, ? extends V> e : m.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

    private V putLocal(K key, V value) {
        if (base.containsKey(key)) {
            throw new IllegalStateException("RelocationMap cannot override existing entry: " + key);
        }
        return base.put(key, value);
    }

    @Deprecated
    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        this.base = (DistMap<K, V>) kryo.readClassAndObject(input);
        this.placeGroup = (TeamedPlaceGroup) kryo.readClassAndObject(input);
        this.distribution = (Distribution<K>) kryo.readClassAndObject(input);
        this.TEAM = new TeamOperations(this);
        this.outputMap = new HashMap<>(placeGroup.size());
        for (final Place pl : placeGroup.places()) {
            final int nThreads = Runtime.getRuntime().availableProcessors();
            outputMap.put(pl, new ConcurrentHashMap<>(nThreads));
        }
    }

    /**
     * Set the rule that defines destination in relocation for entries. The
     * destination of entries that have already been added is not changed.
     */
    public void setDistribution(Distribution<K> dist) {
        distribution = dist;
    }

    @Deprecated
    @Override
    public void write(Kryo kryo, Output output) {
        kryo.writeClassAndObject(output, base);
        kryo.writeClassAndObject(output, placeGroup);
        kryo.writeClassAndObject(output, distribution);
    }
}
