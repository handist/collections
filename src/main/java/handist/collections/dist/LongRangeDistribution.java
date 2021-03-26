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

public class LongRangeDistribution implements RangedDistribution<LongRange> {

    private final HashMap<LongRange, Place> dist;

    /**
     * Copy constructor
     *
     * @param distribution instance to copy
     */
    public LongRangeDistribution(LongRangeDistribution distribution) {
        dist = new HashMap<>(distribution.dist);
    }

    public LongRangeDistribution(Map<LongRange, Place> originalHashMap) {
        dist = new HashMap<>(originalHashMap);
    }

    /**
     * Returns a copy of this instance using the copy constructor
     * {@link #LongRangeDistribution(LongRangeDistribution)}.
     */
    @Override
    public LongRangeDistribution clone() {
        return new LongRangeDistribution(this);
    }

    public HashMap<LongRange, Place> getHashMap() {
        return dist;
    }

    @Override
    public Map<LongRange, Place> placeRanges(LongRange range) {
        final Map<LongRange, Place> listPlaceRange = new HashMap<>();
        for (final LongRange mappedRange : dist.keySet()) {
            final Place mappedPlace = dist.get(mappedRange);
            if (mappedRange.from <= range.from) {
                if (range.from < mappedRange.to) { // if (range.min <= mappedRange.max) {
                    if (range.to <= mappedRange.to) {
                        listPlaceRange.put(range, mappedPlace);
                    } else {
                        listPlaceRange.put(new LongRange(range.from, mappedRange.to), mappedPlace);
                    }
                }
            } else {
                if (mappedRange.from < range.to) { // if (mappedRange.min <= range.max) {
                    if (range.to <= mappedRange.to) {
                        listPlaceRange.put(new LongRange(mappedRange.from, range.to), mappedPlace);
                    } else {
                        listPlaceRange.put(mappedRange, mappedPlace);
                    }
                }
            }
        }
        return listPlaceRange;
    }
}
