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

import java.util.Map;
import java.util.Set;

import apgas.Place;
import handist.collections.LongRange;

/**
 * Interface representing the distribution of a "range" type R over multiple
 * {@link Place}s. This interface consists in a single method, which maps
 * instances of R to places.
 * <p>
 * The type R which represents the "keys" should be recursively splittable into
 * multiple R instances whose union represent the whole set of keys distributed
 * onto the hosts. Possible candidates could be an implementation of
 * {@link Set}, or classes describing value intervals such as {@link LongRange}.
 * <p>
 * For a distribution interface of "single key" kind of implementation, refer to
 * interface {@link Distribution}.
 *
 * @param <R> type used as "key" to describe a distributed collection, a single
 *            instance represents multiple individual keys, contained in a range
 *            of values for instance.
 * @see Distribution
 */
public interface RangedDistribution<R> {

    /**
     * Returns a map of the keys contained in the provided range to the places on
     * which these keys are/should be located.
     * <p>
     * Implementation should ensure that there are no duplicated or overlapping keys
     * in the returned map and that all the contents of the range provided as
     * parameter can be reconstructed by the union of the keys in the returned map.
     *
     * @param range the range or collection of "keys" to map to various places
     * @return a Map from R instances to {@link Place}s
     */
    public Map<R, Place> rangeLocation(R range);
}
