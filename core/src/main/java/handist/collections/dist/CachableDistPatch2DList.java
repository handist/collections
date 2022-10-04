package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;
import handist.collections.patch.Patch2D;
import handist.collections.patch.Position2D;
import handist.collections.patch.Positionable;
import handist.collections.patch.Range2D;

/**
 *
 * Most functions do action on all elements even cached elements. A part of
 * functions (name contains "received", "own") do action on elements that are
 * cached or not cached.
 *
 * @author yoshikikawanishi
 *
 * @param <T>
 */
public class CachableDistPatch2DList<T extends Positionable<Position2D>> extends DistPatch2DList<T> {

    private final TreeMap<Integer, Patch2D<T>> cached;

    public CachableDistPatch2DList(Patch2D<T> patch, int xSplit, int ySplit, TeamedPlaceGroup pg) {
        super(patch, xSplit, ySplit, pg);
        cached = new TreeMap<>();
    }

    public CachableDistPatch2DList(Range2D range, int xSplit, int ySplit, TeamedPlaceGroup pg, GlobalID id) {
        super(range, xSplit, ySplit, pg, id);
        cached = new TreeMap<>();
    }

    /**
     *
     */
    public void clearCache() {
        cached.forEach((id, patch) -> {
            list.remove(id);
        });
        cached.clear();
    }

    public void forEachOwnPatch(Consumer<Patch2D<T>> action) {
        list.forEach((id, patch) -> {
            if (!cached.containsKey(id)) {
                action.accept(patch);
            }
        });
    }

    public void forEachReceivedPatch(Consumer<Patch2D<T>> action) {
        cached.forEach((id, patch) -> {
            action.accept(patch);
        });
    }

    public boolean isCached(Patch2D<T> patch) {
        return cached.containsKey(patch.id());
    }

    /**
     * The elements that will migrate to the cached range will move to the cache
     * owner's place. <b>The elements in the cached patch will not update.</b>
     */
    @Override
    public void migrate() throws Exception {
        // init list for migrate elements
        final Map<Place, List<T>> toMigrate = new HashMap<>(placeGroup().size());
        placeGroup().places.forEach((place) -> {
            toMigrate.put(place, new ArrayList<>());
        });
        // remove migrate elements
        forEachOwnPatch((patch) -> {
            migrate_Remove(patch, toMigrate);
        });
        // put to the local patches or send to the other places.
        final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup());
        toMigrate.forEach((place, list) -> {
            if (place.equals(here())) {
                migrate_ToLocal(list);
            } else {
                migrate_ToRemote(list, place, mm);
            }
        });
        mm.sync();
    }

    /**
     * @param parallelism
     * @param action
     */
    public void parallelForEachOwnPatch(int parallelism, Consumer<Patch2D<T>> action) {
        if (parallelism < 1) {
            throw new IllegalArgumentException();
        }
        finish(() -> {
            final int numPatches = numPatches() - cached.size();
            final int rem = numPatches % parallelism;
            final int quo = numPatches / parallelism;
            final Iterator<Patch2D<T>> pIter = list.values().iterator();

            for (int i = 0; i < parallelism; i++) {
                // the patch count assigned for each thread
                final int nbPatch = (i < rem) ? quo + 1 : quo;
                // get nbPatch count patches from the top.
                final List<Patch2D<T>> splitList = new ArrayList<>();
                for (int j = 0; j < nbPatch; j++) {
                    Patch2D<T> p = pIter.next();
                    // ignore the cached patch
                    while (cached.containsKey(p.id())) {
                        p = pIter.next();
                    }
                    splitList.add(p);
                }
                // do action
                async(() -> {
                    splitList.forEach((patch) -> {
                        action.accept(patch);
                    });
                });
            }
        });
    }

    /**
     * Cache a patch to the other place. If receiver already contains the cached
     * patch, the old one will be replaced.
     *
     * @param patch
     * @param dest
     * @param mm
     */
    public void sharePatchAtSync(Collection<Patch2D<T>> patchSet, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        final Serializer ser = ((s) -> {
            s.writeInt(patchSet.size());
            patchSet.forEach((patch) -> {
                s.writeObject(patch);
            });
        });
        @SuppressWarnings("unchecked")
        final DeSerializer des = ((ds) -> {
            final int n = ds.readInt();
            for (int i = 0; i < n; i++) {
                final Patch2D<T> recv = (Patch2D<T>) ds.readObject();
                cached.put(recv.id(), recv);
                list.put(recv.id(), recv);
            }
        });
        mm.request(dest, ser, des);
    }

    /**
     * Cache a patch to the other place. If receiver already contains the cached
     * patch, the old one will be replaced.
     *
     * @param patch
     * @param dest
     * @param mm
     */
    public void sharePatchAtSync(Patch2D<T> patch, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        final Serializer ser = ((s) -> {
            s.writeObject(patch);
        });
        @SuppressWarnings("unchecked")
        final DeSerializer des = ((ds) -> {
            final Patch2D<T> recv = (Patch2D<T>) ds.readObject();
            cached.put(recv.id(), recv);
            list.put(recv.id(), patch);
        });
        mm.request(dest, ser, des);
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup();
        final GlobalID id1 = id();
        final Range2D r1 = allRange;
        final int xs = xSplit;
        final int ys = ySplit;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new CachableDistPatch2DList<>(r1, xs, ys, pg1, id1);
        });
    }

}
