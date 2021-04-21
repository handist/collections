package handist.collections.dist;

import java.util.concurrent.ConcurrentHashMap;

import apgas.util.GlobalID;

public class DistConcurrentMultiMap<K, V> extends DistMultiMap<K, V> {

    /**
     * Construct a DistConcurrentMultiMap.
     */
    public DistConcurrentMultiMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistConcurrentMultiMap with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistConcurrentMultiMap(TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Construct a DistConcurrentMultiMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     */
    public DistConcurrentMultiMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id);
        this.data = new ConcurrentHashMap<>();
    }

}
