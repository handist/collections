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

public class LongDistribution implements Distribution<Long> {

    /**
     *
     */
    private static final long serialVersionUID = 7043702534459848203L;

    public static LongDistribution convert(Map<LongRange, Place> rangedHashMap) {
        final HashMap<Long, Place> newHashMap = new HashMap<>();
        for (final Map.Entry<LongRange, Place> entry : rangedHashMap.entrySet()) {
            final LongRange range = entry.getKey();
            final Place place = entry.getValue();
            for (Long i = range.from; i < range.to; i++) {
                newHashMap.put(i, place);
            }
        }
        return new LongDistribution(newHashMap);
    }

    private final HashMap<Long, Place> dist;

    public LongDistribution(LongDistribution distribution) {
        dist = cloneHashMap(distribution.getHashMap());
    }

    public LongDistribution(Map<Long, Place> originalHashMap) {
        dist = cloneHashMap(originalHashMap);
    }

    @Override
    public LongDistribution clone() {
        return new LongDistribution(this);
    }

    private HashMap<Long, Place> cloneHashMap(Map<Long, Place> originalHashMap) {
        final HashMap<Long, Place> newHashMap = new HashMap<>();
        for (final Map.Entry<Long, Place> entry : originalHashMap.entrySet()) {
            newHashMap.put(entry.getKey(), entry.getValue());
        }
        return newHashMap;
    }

    public HashMap<Long, Place> getHashMap() {
        return dist;
    }

    @Override
    public Place place(Long key) {
        return dist.get(key);
    }

    @Override
    public String toString() {
        return "[LongDistribution]" + dist;
    }
}
