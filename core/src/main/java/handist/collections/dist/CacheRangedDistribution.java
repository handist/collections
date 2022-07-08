package handist.collections.dist;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import apgas.Place;

public interface CacheRangedDistribution<R> extends Serializable {

    /**
     * Returns a map of the keys contained in the provided range to the places on
     * which these keys are/should be cached.
     * <p>
     * Implementation should ensure that there are no duplicated or overlapping keys
     * in the returned map and that all the contents of the range provided as
     * parameter can be reconstructed by the union of the keys in the returned map.
     *
     * @param range the range or collection of "keys" to map to various places
     * @return a Map from R instances to {@link Place}s
     */
    public Map<R, Collection<Place>> rangeLocation(R range);
}
