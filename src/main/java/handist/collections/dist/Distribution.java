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

import java.io.Serializable;

import apgas.Place;

/**
 * Represents the mapping of a key to a place for a distributed collection. This
 * interface proposes a single method which assigns a {@link Place} to a key.
 * Distributed collection facilities will use implementations of this interface
 * by interrogating the {@link #place(Object)} method with the keys they are
 * manipulating to identify on which host the associated value should be
 * located.
 * <p>
 * For a distribution created from objects that represent a range of keys, refer
 * to interface {@link RangedDistribution}.
 *
 * @param <K> the type used as key of a distributed collection
 * @see RangedDistribution
 */
public interface Distribution<K> extends Serializable {

    public Place place(K key);

    /**
     * Apply the given function to the elements(keys) of the distribuiton.
     *
     * @param func defines the behavior for the geven key:K and its location p:
     *             Place.
     */
    // public map(func: (key:K, p: Place) => void): void;

    /**
     * Apply the given function to the elements(keys) that should be assigned to the
     * specifiedplace
     *
     * @param place the destination place
     * @param func  defines the behavior for the geven key:K
     */
    // public map(place:Place, func: (key:K) => void): void;
}
