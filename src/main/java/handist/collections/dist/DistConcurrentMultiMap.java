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

import java.io.ObjectStreamException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;

public class DistConcurrentMultiMap<K, V> extends DistMultiMap<K, V> {

    /**
     * Construct a DistConcurrentMultiMap.
     */
    public DistConcurrentMultiMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistConcurrentMultiMap with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistConcurrentMultiMap(TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Construct a DistConcurrentMultiMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     */
    public DistConcurrentMultiMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        this(placeGroup, id, new ConcurrentHashMap<>());

    }
    protected DistConcurrentMultiMap(TeamedPlaceGroup placeGroup, GlobalID id, Map<K,Collection<V>> data) {
        super(placeGroup, id, data);
        super.GLOBAL = new GlobalOperations<>(this, (TeamedPlaceGroup pg0, GlobalID gid) -> new DistConcurrentMultiMap<>(pg0, gid));
    }

    @Override
    protected Collection<V> createEmptyCollection() {
        return new ConcurrentLinkedQueue<>();
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistConcurrentMultiMap<>(pg1, id1);
        });
    }

}
