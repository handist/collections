package handist.util.dist;

import java.util.HashMap;
import java.util.Map;

import apgas.Place;
import handist.util.LongRange;

public class DistributionLong implements Distribution<Long> {

        private HashMap<Long, Place> dist;

        public DistributionLong(DistributionLong distribution) {
            dist = cloneHashMap(distribution.getHashMap());
        }

        public DistributionLong(Map<Long,Place> originalHashMap) {
            dist = cloneHashMap(originalHashMap);
        }

        public static DistributionLong convert(Map<LongRange,Place> rangedHashMap) {
            HashMap<Long,Place> newHashMap = new HashMap<>();
            for (Map.Entry<LongRange, Place> entry: rangedHashMap.entrySet()) {
                LongRange range = entry.getKey();
                Place place = entry.getValue();
                for (Long i=range.from; i<range.to; i++) {
                    newHashMap.put(i, place);
                }
            }
            return new DistributionLong(newHashMap);
        }

        public DistributionLong clone() {
            return new DistributionLong(this);
        }

        private HashMap<Long,Place> cloneHashMap(Map<Long,Place> originalHashMap) {
            HashMap<Long,Place> newHashMap = new HashMap<>();
            for (Map.Entry<Long, Place> entry: originalHashMap.entrySet()) {
                newHashMap.put(entry.getKey(), entry.getValue());
            }
            return newHashMap;
        }

        public Place place(Long key) {
            return dist.get(key);
        }

        public HashMap<Long,Place> getHashMap() {
            return dist;
        }
}
