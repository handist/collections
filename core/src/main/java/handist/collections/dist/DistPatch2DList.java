package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.function.DeSerializer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.Serializer;
import handist.collections.patch.Index2D;
import handist.collections.patch.Patch2D;
import handist.collections.patch.Patch2DList;
import handist.collections.patch.Position2D;
import handist.collections.patch.Positionable;
import handist.collections.patch.Range2D;

public class DistPatch2DList<T extends Positionable<Position2D>> extends Patch2DList<T>
        implements DistributedCollection<T, DistPatch2DList<T>>, ElementLocationManageable<Index2D> {

    /** */
    private final TeamedPlaceGroup placeGroup;
    /** */
    private final GlobalID globalId;
    /** */
    public transient float[] locality;
    /** */
    public final GlobalOperations<T, DistPatch2DList<T>> GLOBAL;
    /** */
    public final TeamOperations<T, DistPatch2DList<T>> TEAM;
    /** */
    @SuppressWarnings("rawtypes")
    private DistCollectionSatellite satellite;
    /** */
    private final ElementLocationManager<Index2D> pdist;

    /**
     * @param patch
     * @param xSplit
     * @param ySplit
     * @param pg
     */
    @SuppressWarnings("deprecation")
    public DistPatch2DList(Patch2D<T> patch, int xSplit, int ySplit, TeamedPlaceGroup pg) {
        this(patch.getRange(), xSplit, ySplit, pg, new GlobalID());
        // init list
        int i = 0;
        for (final Patch2D<T> p : patch.split(xSplit, ySplit)) {
            p.setId(i);
            list.put(i, p);
            pdist.add(getIndex(p));
            i++;
        }
    }

    /**
     * Constructor for writeReplace. Create the empty list.
     */
    protected DistPatch2DList(Range2D range, int xSplit, int ySplit, TeamedPlaceGroup pg, GlobalID id) {
        super(range, xSplit, ySplit);
        placeGroup = pg;
        globalId = id;
        locality = new float[pg.size()];
        GLOBAL = new GlobalOperations<>(this,
                (TeamedPlaceGroup pg0, GlobalID gid) -> new DistPatch2DList<>(range, xSplit, ySplit, pg0, gid));
        TEAM = new TeamOperations<>(this);
        pdist = new ElementLocationManager<>();
        id.putHere(this);
        Arrays.fill(locality, 1.0f);
    }

    @Override
    public void forEach(SerializableConsumer<T> action) {
        super.forEach(action);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends DistCollectionSatellite<DistPatch2DList<T>, S>> S getSatellite() {
        return (S) satellite;
    }

    @Override
    public void getSizeDistribution(long[] result) {
        for (final Map.Entry<Index2D, Place> entry : pdist.dist.entrySet()) {
            final Index2D k = entry.getKey();
            final Place p = entry.getValue();
            result[placeGroup.rank(p)]++;
        }
    }

    @Override
    public GlobalOperations<T, DistPatch2DList<T>> global() {
        return GLOBAL;
    }

    @Override
    public GlobalID id() {
        return globalId;
    }

    @Override
    public float[] locality() {
        return locality;
    }

    /**
     * @param index
     * @return
     */
    public Place locationOf(Index2D patchIndex) {
        return pdist.dist.get(patchIndex);
    }

    /**
     * @param index
     * @return
     */
    public Place locationOf(Position2D position) {
        return pdist.dist.get(getIndex(position));
    }

    @Override
    public long longSize() {
        return size();
    }

    /**
     * if moved patches after the latest migration anywhere, should call
     * {@link #updateDist}.
     */
    @Override
    public void migrate() throws Exception {
        // init list for migrate elements
        final Map<Place, List<T>> toMigrate = new HashMap<>(placeGroup.size());
        placeGroup.places.forEach((place) -> {
            toMigrate.put(place, new ArrayList<>());
        });
        // remove migrate elements
        forEachPatch((patch) -> {
            migrate_Remove(patch, toMigrate);
        });
        // put to the local patches or send to the other places.
        final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
        toMigrate.forEach((place, list) -> {
            if (place.equals(here())) {
                migrate_ToLocal(list);
            } else {
                migrate_ToRemote(list, place, mm);
            }
        });
        mm.sync();
    }

    protected void migrate_Remove(Patch2D<T> patch, Map<Place, List<T>> toMigrate) {
        final Iterator<T> iter = patch.iterator();
        while (iter.hasNext()) {
            final T t = iter.next();
            if (patch.getRange().contains(t.position())) {
                continue;
            }
            final Place dest = locationOf(t.position());
            toMigrate.get(dest).add(t);
            iter.remove();
        }
    }

    protected void migrate_ToLocal(List<T> data) {
        data.forEach((t) -> {
            put(t);
        });
    }

    protected void migrate_ToRemote(List<T> data, Place dest, CollectiveMoveManager mm) {
        final DistPatch2DList<T> toBranch = this;
        final Serializer ser = ((s) -> {
            s.writeObject(data);
        });
        @SuppressWarnings("unchecked")
        final DeSerializer des = ((ds) -> {
            final List<T> recv = (List<T>) ds.readObject();
            recv.forEach((t) -> {
                toBranch.put(t);
            });
        });
        mm.request(dest, ser, des);
    }

    /**
     * @param distribution
     * @param mm
     */
    public void moveAtSync(Distribution<Index2D> distribution, MoveManager mm) {
        forEachPatch((patch) -> {
            moveAtSync(patch, distribution.location(getIndex(patch)), mm);
        });
    }

    /**
     * @param patch
     * @param dest
     * @param mm
     */
    public void moveAtSync(Patch2D<T> patch, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        if (!list.containsKey(patch.id())) {
            return;
        }
        final DistPatch2DList<T> toBranch = this;

        final Serializer ser = ((s) -> {
            final byte keyType = pdist.moveOut(getIndex(patch), dest);
            removeForMove(patch);
            s.writeByte(keyType);
            s.writeObject(patch);
        });

        @SuppressWarnings("unchecked")
        final DeSerializer des = ((ds) -> {
            final byte keyType = ds.readByte();
            final Patch2D<T> p = (Patch2D<T>) ds.readObject();
            toBranch.putForMove(p, keyType);
        });
        mm.request(dest, ser, des);
    }

    @Override
    public void moveAtSyncCount(ArrayList<IntLongPair> moveList, MoveManager mm) throws Exception {
        // TODO Auto-generated method stub
    }

    @Override
    public void parallelForEach(SerializableConsumer<T> action) {
        // TODO Auto-generated method stub
    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    /**
     * @param patch
     * @throws Exception
     */
    private void putForMove(Patch2D<T> patch, byte mType) throws Exception {
        list.put(patch.id(), patch);
        final Index2D key = getIndex(patch);
        switch (mType) {
        case ElementLocationManager.MOVE_NEW:
            pdist.moveInNew(key);
            break;
        case ElementLocationManager.MOVE_OLD:
            pdist.moveInOld(key);
            break;
        default:
            throw new Exception("SystemError when calling putForMove " + key);
        }
    }

    @Override
    public void registerDistribution(UpdatableDistribution<Index2D> distributionToUpdate) {
        pdist.registerDistribution(distributionToUpdate);
    }

    /**
     * @param patch
     */
    private void removeForMove(Patch2D<T> patch) {
        list.remove(patch.id());
    }

    @Override
    public <S extends DistCollectionSatellite<DistPatch2DList<T>, S>> void setSatellite(S satellite) {
        this.satellite = satellite;
    }

    @Override
    public TeamOperations<T, DistPatch2DList<T>> team() {
        return TEAM;
    }

    @Override
    public void updateDist() {
        pdist.update(placeGroup);

    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = globalId;
        final Range2D r1 = allRange;
        final int xs = xSplit;
        final int ys = ySplit;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistPatch2DList<>(r1, xs, ys, pg1, id1);
        });
    }

}
