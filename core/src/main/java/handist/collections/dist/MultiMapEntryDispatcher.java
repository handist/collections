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
import java.util.Collection;
import java.util.Map;

import apgas.Place;
import handist.collections.MultiMap;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

/**
 *
 * {@link MultiMap} has this class in order to dispatch entris to places defined
 * by {@link Distribution}. Relocate entries between places by calling
 * {@link MapEntryDispatcher.TeamOperations#dispatch} The dispatched entries are
 * added to original {@link MultiMap}.
 *
 * Please be careful that reference relationships like multiple references to
 * one object are not maintained.
 *
 * @author yoshikikawanishi
 *
 * @param <K> : The key type of map entry
 * @param <V> : The value type of map entry
 */
public class MultiMapEntryDispatcher<K, V> extends MapEntryDispatcher<K, Collection<V>> {

    private static final long serialVersionUID = -8897691961652214931L;

    /**
     * @param distMap
     * @param pg
     * @param dist
     */
    MultiMapEntryDispatcher(MultiMap<K, V> multiMap, TeamedPlaceGroup pg, Distribution<K> dist) {
        super(multiMap, pg, dist);
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
                    put1Local(key, value); // NOTE : If put1 of MultiMap becomes put, it is not necessary to prepare
                                           // this class.
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
     * Put a new entry. The entries are relocated when #relocate is called.
     *
     * @param key   the key of the new entry.
     * @param value the value of the new entry.
     * @return If the destination associated with the key is here, return the value.
     *         If not, return null.
     */
    public boolean put1(K key, V value) {
        final Place next = distribution.location(key);
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
        return ((MultiMap<K, V>) base).put1(key, value);
    }

    /**
     * Unsupported.
     */
    @Override
    @Deprecated
    public void putAll(Map<? extends K, ? extends Collection<V>> m) {
        throw new UnsupportedOperationException();
    }

}
