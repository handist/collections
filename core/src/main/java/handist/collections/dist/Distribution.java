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
 * by interrogating the {@link #location(Object)} method with the keys they are
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

    /**
     * Returns the location of an entry in a distribution
     *
     * @param key object used to identify an entry in the distributed collection
     * @return the location of the entry as recorded by this distribution, null if
     *         the enquired entry is unknown to this distribution
     */
    public Place location(K key);
}
