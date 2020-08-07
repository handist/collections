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

import java.util.HashMap;
import java.util.Map;

import apgas.Place;
import handist.collections.LongRange;

public class LongDistribution implements Distribution<Long> {

	public static LongDistribution convert(Map<LongRange,Place> rangedHashMap) {
		HashMap<Long,Place> newHashMap = new HashMap<>();
		for (Map.Entry<LongRange, Place> entry: rangedHashMap.entrySet()) {
			LongRange range = entry.getKey();
			Place place = entry.getValue();
			for (Long i=range.from; i<range.to; i++) {
				newHashMap.put(i, place);
			}
		}
		return new LongDistribution(newHashMap);
	}

	private HashMap<Long, Place> dist;

	public LongDistribution(LongDistribution distribution) {
		dist = cloneHashMap(distribution.getHashMap());
	}

	public LongDistribution(Map<Long,Place> originalHashMap) {
		dist = cloneHashMap(originalHashMap);
	}

	public LongDistribution clone() {
		return new LongDistribution(this);
	}

	private HashMap<Long,Place> cloneHashMap(Map<Long,Place> originalHashMap) {
		HashMap<Long,Place> newHashMap = new HashMap<>();
		for (Map.Entry<Long, Place> entry: originalHashMap.entrySet()) {
			newHashMap.put(entry.getKey(), entry.getValue());
		}
		return newHashMap;
	}

	public HashMap<Long,Place> getHashMap() {
		return dist;
	}

	public Place place(Long key) {
		return dist.get(key);
	}
	@Override
	public String toString() {
		return "[LongDistribution]" + dist;
	}
}
