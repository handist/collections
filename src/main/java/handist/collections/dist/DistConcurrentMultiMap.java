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

import java.util.concurrent.ConcurrentHashMap;

import apgas.util.GlobalID;

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
        super(placeGroup, id);
        data = new ConcurrentHashMap<>();
    }

}
