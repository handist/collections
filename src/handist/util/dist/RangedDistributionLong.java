package handist.util.dist;

import java.util.HashMap;
import java.util.Map;

import apgas.Place;
import handist.util.LongRange;

public class RangedDistributionLong implements RangedDistribution<LongRange> {

    private HashMap<LongRange, Place> dist;

    public RangedDistributionLong(RangedDistributionLong distribution) {
        dist = new HashMap<LongRange, Place>(distribution.getHashMap());
    }

    public RangedDistributionLong(Map<LongRange, Place> originalHashMap) {
		dist = new HashMap<>(originalHashMap);
    }

    public RangedDistributionLong clone() {
        return new RangedDistributionLong(this);
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

    public HashMap<LongRange, Place> getHashMap() {
        return dist;
    }
}
