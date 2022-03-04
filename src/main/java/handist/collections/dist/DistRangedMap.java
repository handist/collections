package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.function.Function;

import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.LongRange;
import handist.collections.RangedMap;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

/**
 *
 *
 * TODO : handle not for LongRange but for object range.
 *
 * @author yoshikikawanishi
 */
public class DistRangedMap<T> extends RangedMap<T> implements DistributedCollection<T, DistRangedMap<T>>,
        RangeRelocatable<LongRange>, ElementLocationManageable<LongRange>, SerializableWithReplace {

    /**
     * Internal class that handles distribution-related operations.
     */
    protected final transient ElementLocationManager<LongRange> ldist;

    /** Handle for GLB operations */
    // public final DistMapGlb<K, V> GLB;

    public GlobalOperations<T, DistRangedMap<T>> GLOBAL;

    final GlobalID id;

    public transient float[] locality;

    public final TeamedPlaceGroup placeGroup;

    protected final TeamOperations<T, DistRangedMap<T>> TEAM;

    @SuppressWarnings("rawtypes")
    private DistCollectionSatellite satellite;

    /**
     * Construct a DistRangedMap.
     */
    public DistRangedMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistRangedMap with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistRangedMap(TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Construct a DistRangedMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     */
    public DistRangedMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        super();
        ldist = new ElementLocationManager<>();
        this.placeGroup = placeGroup;
        this.id = id;
        locality = new float[placeGroup.size];
        Arrays.fill(locality, 1.0f);
        this.GLOBAL = new GlobalOperations<>(this,
                (TeamedPlaceGroup pg0, GlobalID gid) -> new DistRangedMap<>(pg0, gid));
        // GLB = new DistMapGlb<>(this);
        TEAM = new TeamOperations<>(this);
        id.putHere(this);
    }

    private void addForMove(LongRange range, byte mType) throws Exception {
        switch (mType) {
        case ElementLocationManager.MOVE_NEW:
            ldist.moveInNew(range);
            break;
        case ElementLocationManager.MOVE_OLD:
            ldist.moveInOld(range);
            break;
        default:
            throw new Exception("SystemError when calling addForMove " + range);
        }
        super.addRange(range);
    }

    @Override
    public void addRange(LongRange range) {
        super.addRange(range);
        ldist.add(range);
    }

    @Override
    public void addRange(LongRange range, Function<Long, T> func) {
        super.addRange(range, func);
        ldist.add(range);
    }

    @Override
    public Collection<LongRange> getAllRanges() {
        return ranges;
    }

    /**
     * Returns a newly created snapshot of the current distribution of registered
     * tracking ranges as a {@link LongRangeDistribution}. This returned
     * distribution's contents will become out-of-date if the contents of this class
     * are relocated, added, and/or removed.
     *
     * @return a new {@link LongRangeDistribution} object representing the current
     *         distribution of this collection
     */
    public LongRangeDistribution getDistribution() {
        return new LongRangeDistribution(ldist.dist);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends DistCollectionSatellite<DistRangedMap<T>, S>> S getSatellite() {
        return (S) satellite;
    }

    @Override
    public void getSizeDistribution(long[] result) {
        for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
            final LongRange k = entry.getKey();
            final Place p = entry.getValue();
            result[placeGroup.rank(p)] += k.size();
        }
    }

    @Override
    public GlobalOperations<T, DistRangedMap<T>> global() {
        return GLOBAL;
    }

    @Override
    public GlobalID id() {
        return id;
    }

    @Override
    public float[] locality() {
        return locality;
    }

    @Override
    public long longSize() {
        return size();
    }

    @Override
    public void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManager mm) throws Exception {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public void moveRangeAtSync(LongRange range, Place dest, MoveManager manager) {
        if (dest.equals(here())) {
            return;
        }
        final DistRangedMap<T> toBranch = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final RangedMap<T> toMove = toBranch.split(range);
            s.writeInt(toMove.ranges().size());
            for (final LongRange r : toMove.ranges()) {
                final byte mType = ldist.moveOut(r, dest);
                s.writeObject(r);
                s.writeByte(mType);
            }

            final ConcurrentNavigableMap<Long, T> sub = data.subMap(range.from, range.to);
            final int num = sub.size();
            s.writeInt(num);
            final Iterator<Entry<Long, T>> iter = sub.entrySet().iterator();
            while (iter.hasNext()) {
                final long key = iter.next().getKey();
                final T value = this.removeForMove(key);
                s.writeLong(key);
                s.writeObject(value);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int rangesNum = ds.readInt();
            for (int i = 0; i < rangesNum; i++) {
                final LongRange r = (LongRange) ds.readObject();
                final byte mType = ds.readByte();
                toBranch.addForMove(r, mType);
            }

            final int num = ds.readInt();
            for (int i = 0; i < num; i++) {
                final long index = ds.readLong();
                @SuppressWarnings("unchecked")
                final T t = (T) ds.readObject();
                toBranch.put(index, t);
            }
        };
        manager.request(dest, serialize, deserialize);
    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    @Override
    public void registerDistribution(UpdatableDistribution<LongRange> distributionToUpdate) {
        ldist.registerDistribution(distributionToUpdate);
    }

    private T removeForMove(Long i) {
        final T t = super.remove(i);
        if (t == null) {
            throw new NullPointerException("removeForMove null pointer value of index: " + i);
        }
        return t;
    }

    @Override
    protected boolean removeRangeTemporary(LongRange range) {
        ldist.remove(range);
        return ranges.remove(range);
    }

    @Override
    public <S extends DistCollectionSatellite<DistRangedMap<T>, S>> void setSatellite(S satellite) {
        this.satellite = satellite;
    }

    @Override
    public TeamOperations<T, DistRangedMap<T>> team() {
        return TEAM;
    }

    @Override
    public void updateDist() {
        ldist.update(placeGroup);
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistRangedMap<>(pg1, id1);
        });
    }

}
