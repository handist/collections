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

import java.util.HashMap;
import java.util.Map;

import apgas.Place;
import handist.collections.LongRange;

/**
 * {@link Distribution}&lt;Long&gt; implementation which records a distribution
 * into a {@link Map}&lt;{@link Long}, {@link Place}&gt;.
 */
public class LongDistribution extends UpdatableDistribution<Long> implements Distribution<Long> {

    /** Serial Version UID */
    private static final long serialVersionUID = 7043702534459848203L;

    /**
     * Utility method used to copy a provided map
     *
     * @param map map whose contents need to be copied
     * @return new {@link HashMap} containing a copy of each mapping contained in
     *         the provided map
     */
    private static HashMap<Long, Place> cloneIntoHashMap(Map<Long, Place> map) {
        final HashMap<Long, Place> newHashMap = new HashMap<>();
        for (final Map.Entry<Long, Place> entry : map.entrySet()) {
            newHashMap.put(entry.getKey(), entry.getValue());
        }
        return newHashMap;
    }

    /**
     * Converts a Map from {@link LongRange} to {@link Place} into an explicit
     * {@link LongDistribution} with a mapping for each individual index contained
     * in the ranges of the provided map.
     *
     * @param map map from ranges to places to convert to an explicit
     * @return a new {@link LongDistribution} based on the provided map
     */
    public static LongDistribution convert(Map<LongRange, Place> map) {
        final HashMap<Long, Place> dist = new HashMap<>();
        for (final Map.Entry<LongRange, Place> entry : map.entrySet()) {
            final LongRange range = entry.getKey();
            final Place place = entry.getValue();
            for (Long i = range.from; i < range.to; i++) {
                dist.put(i, place);
            }
        }
        return new LongDistribution(dist);
    }

    /** Internal map used to record the distribution */
    private final HashMap<Long, Place> distribution;

    /**
     * Default constructor.
     * <p>
     * Creates a blank distribution which may be registered with
     * {@link DistIdMap#registerDistribution(UpdatableDistribution)} to be updated
     * with that collection's distribution automatically.
     */
    public LongDistribution() {
        distribution = new HashMap<>();
    }

    /**
     * Internal-use constructor
     *
     * @param map the map to use to build the {@link LongDistribution}
     */
    protected LongDistribution(HashMap<Long, Place> map) {
        distribution = map;
    }

    /**
     * Creates a new {@link LongDistribution} based on the given mappings from
     * {@link Long} to {@link Place}
     * <p>
     * A copy of the map given as parameter is created. Changes to the created
     * {@link LongDistribution} <em>will not</em> be reflected into the map given as
     * parameter and vice-versa.
     *
     * @param map mappings of Long indices to Place based on which to construct a
     *            new {@link LongDistribution}
     */
    public LongDistribution(Map<Long, Place> map) {
        distribution = cloneIntoHashMap(map);
    }

    /**
     * Creates a copy of this {@link LongDistribution}. Any subsequent change to
     * this distribution or the one returned as parameter will not be reflected into
     * the other instance.
     */
    @Override
    public LongDistribution clone() {
        return new LongDistribution(distribution);
    }

    @Override
    public Place location(Long key) {
        return distribution.get(key);
    }

    @Override
    void removeLocation(Long k) {
        distribution.remove(k);
    }

    @Override
    public String toString() {
        return "[LongDistribution]" + distribution;
    }

    @Override
    void updateLocation(Long k, Place location) {
        distribution.put(k, location);
    }
}
