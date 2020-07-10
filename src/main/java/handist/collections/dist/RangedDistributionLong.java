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

public class RangedDistributionLong implements RangedDistribution<LongRange> {

	private HashMap<LongRange, Place> dist;

	public RangedDistributionLong(Map<LongRange, Place> originalHashMap) {
		dist = new HashMap<>(originalHashMap);
	}

	public RangedDistributionLong(RangedDistributionLong distribution) {
		dist = new HashMap<LongRange, Place>(distribution.getHashMap());
	}

	public RangedDistributionLong clone() {
		return new RangedDistributionLong(this);
	}

	public HashMap<LongRange, Place> getHashMap() {
		return dist;
	}

	public Map<LongRange, Place> placeRanges(LongRange range) {
		Map<LongRange,Place> listPlaceRange = new HashMap<>();
		for (LongRange mappedRange: dist.keySet()) {
			Place mappedPlace = dist.get(mappedRange);
			if (mappedRange.from <= range.from) {
				if (range.from < mappedRange.to) { //if (range.min <= mappedRange.max) {
					if (range.to <= mappedRange.to) {
						listPlaceRange.put(range, mappedPlace);
					} else {
						listPlaceRange.put(new LongRange(range.from, mappedRange.to), mappedPlace);
					}
				}
			} else {
				if (mappedRange.from < range.to) { //if (mappedRange.min <= range.max) {
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
