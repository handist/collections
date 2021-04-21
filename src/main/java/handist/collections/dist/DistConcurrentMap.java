package handist.collections.dist;

import java.util.concurrent.ConcurrentHashMap;

import apgas.util.GlobalID;

public class DistConcurrentMap<K, V> extends DistMap<K, V> {

    /**
     * Construct an empty DistConcurrentMap which can have local handles on all the
     * hosts in the computation.
     */
    public DistConcurrentMap() {
        this(TeamedPlaceGroup.world);
    }

    /**
     * Construct a DistMap which can have local handles on the hosts of the
     * specified {@link TeamedPlaceGroup}.
     *
     * @param pg the group of hosts that are susceptible to manipulate this
     *           {@link DistConcurrentMap}
     */
    public DistConcurrentMap(TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }

    /**
     * Package private DistConcurrentMap constructor. This constructor is used to
     * register a new DistConcurrentMap handle with the specified GlobalId.
     * Programmers that use this library should never have to call this constructor.
     * <p>
     * Specifying a GLobalId which already has object handles registered in other
     * places (potentially objects different from a {@link DistConcurrentMap}
     * instance) could prove disastrous. Instead, programmers should only call
     * {@link #DistConcurrentMap()} to create a distributed map with handles on all
     * hosts, or {@link #DistConcurrentMap(TeamedPlaceGroup)} to restrict their
     * DistMap to a subset of hosts.
     *
     * @param pg       the palceGroup on which this DistConcurrentMap is defined
     * @param globalId the global id associated to this distributed map
     */
    DistConcurrentMap(TeamedPlaceGroup pg, GlobalID globalId) {
        super(pg, globalId);
        this.data = new ConcurrentHashMap<>();
    }

}
