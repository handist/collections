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
		    if (mappedRange.begin <= range.begin) {
	        	if (range.begin < mappedRange.end) { //if (range.min <= mappedRange.max) {
			    	if (range.end <= mappedRange.end) {
			        	listPlaceRange.put(range, mappedPlace);
		    		} else {
						listPlaceRange.put(new LongRange(range.begin, mappedRange.end), mappedPlace);
		    		}
				}
		    } else {
				if (mappedRange.begin < range.end) { //if (mappedRange.min <= range.max) {
				    if (range.end <= mappedRange.end) {
		       			listPlaceRange.put(new LongRange(mappedRange.begin, range.end), mappedPlace);
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
