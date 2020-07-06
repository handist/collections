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

import apgas.Place;

public interface Distribution<K> /* implements Map[K,Place] */{

    public Place place(K key);

    /**
     * Apply the given function to the elements(keys) of the distribuiton.
     *
     * @param func defines the behavior for the geven key:K and its location p: Place.
     */
    //public map(func: (key:K, p: Place) => void): void;

    /**
     * Apply the given function to the elements(keys) that should be assigned to the specifiedplace
     *
     * @param place the destination place
     * @param func defines the behavior for the geven key:K
     */
    //public map(place:Place, func: (key:K) => void): void;
}
