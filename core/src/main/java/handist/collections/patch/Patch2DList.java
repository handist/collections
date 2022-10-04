package handist.collections.patch;

import static apgas.Constructs.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * The class split one 2d space into grid patches equally .
 *
 * @author yoshikikawanishi
 *
 * @param <T>
 */
public class Patch2DList<T extends Positionable<Position2D>> implements Serializable {

    public enum Edge {
        wall, loop
    }

    private static final long serialVersionUID = -2713485949079921703L;
    /** the array for a grid patches */
    protected final TreeMap<Integer, Patch2D<T>> list;
    /** all space before the split into patches */
    protected final Range2D allRange;
    /** the number of grid */
    protected final int xSplit, ySplit;

    public Patch2DList(Patch2D<T> patch, int xSplit, int ySplit) {
        this(patch.range, xSplit, ySplit);
        // init list
        int i = 0;
        for (final Patch2D<T> p : patch.split(xSplit, ySplit)) {
            p.setId(i);
            list.put(i, p);
            i++;
        }
    }

    /**
     * Constructor for writeReplace. Create the empty array.
     */
    protected Patch2DList(Range2D allRange, int xSplit, int ySplit) {
        this.xSplit = xSplit;
        this.ySplit = ySplit;
        this.allRange = allRange;
        list = new TreeMap<>();
    }

    /**
     *
     */
    public void clear() {
        forEachPatch((patch) -> {
            patch.clear();
        });
    }

    /**
     * @param position
     * @return
     */
    public boolean contains(T t) {
        final Patch2D<T> patch = getPatch(t.position());
        return patch.contains(t);
    }

    /**
     * @param patch
     * @return
     */
    public boolean containsPatch(Patch2D<T> patch) {
        return list.containsKey(patch.id);
    }

    /**
     * @param range
     * @return
     */
    public boolean containsRange(Range2D range) {
        return (getPatch(range) != null);
    }

    /**
     *
     * @param center
     * @param distance
     * @return the collections of neighbor patches contains own.
     */
    public Collection<Patch2D<T>> findNeighbors(Patch2D<T> center, int distance, Edge edge) {
        final Collection<Patch2D<T>> result = new ArrayList<>();
        final Index2D cId = getIndex(center);
        findNeighborsImpl(cId, distance, edge, (x, y) -> {
            final Patch2D<T> p = getPatch(x, y);
            if (p != null && !result.contains(p)) {
                result.add(p);
            }
        });
        return result;
    }

    private void findNeighborsImpl(Index2D cId, int distance, Edge edge, BiConsumer<Integer, Integer> action) {
        for (int yDiff = -distance; yDiff <= distance; yDiff++) {
            final int y = wrappedY(cId.y + yDiff, edge);
            for (int xDiff = -distance; xDiff <= distance; xDiff++) {
                final int x = wrappedX(cId.x + xDiff, edge);
                action.accept(x, y);
            }
        }
    }

    /**
     * Returns the list of neighbor indexes even the patch for index is not
     * contained.
     *
     * @param cId
     * @param distance
     * @param edge
     * @return
     */
    public Collection<Index2D> findNeighborsIndex(Patch2D<T> center, int distance, Edge edge) {
        final Collection<Index2D> result = new ArrayList<>();
        final Index2D cId = getIndex(center);
        findNeighborsImpl(cId, distance, edge, (x, y) -> {
            final Index2D i = new Index2D(x, y);
            if (!result.contains(i)) {
                result.add(i);
            }
        });
        return result;
    }

    /**
     * Finds a collection of patches overlapping the given range.
     *
     * @param range
     * @param inclusive
     * @return
     */
    public Collection<Patch2D<T>> findPatches(Range2D range) {
        final Collection<Patch2D<T>> result = new ArrayList<>();
        final Index2D leftBottom = getIndex(range.leftBottom);
        final Index2D rightTop = getIndex(range.rightTop);
        for (int x = leftBottom.x; x < rightTop.x; x++) {
            for (int y = leftBottom.y; y < rightTop.y; y++) {
                final Patch2D<T> p = getPatch(x, y);
                if (p != null) {
                    result.add(p);
                }
            }
        }
        return result;
    }

    /**
     * @param func
     */
    public void forEach(Consumer<T> func) {
        list.forEach((i, patch) -> {
            patch.forEach(func);
        });
    }

    /**
     * @param func
     */
    public void forEachPatch(Consumer<Patch2D<T>> func) {
        list.forEach((i, patch) -> {
            func.accept(patch);
        });
    }

    /**
     * @param patch
     * @return
     */
    public Index2D getIndex(Patch2D<T> patch) {
        if (!list.containsValue(patch)) {
            throw new IllegalArgumentException("Not contains patch " + patch);
        }
        return new Index2D(patch.id % xSplit, patch.id / xSplit);
    }

    /**
     * @param position
     * @return
     */
    public Index2D getIndex(Position2D position) {
        final Position2D size = gridSize();
        return new Index2D((int) (position.x / size.x), (int) (position.y / size.y));
    }

    public Index2D getIndex_unchecked(Patch2D<T> patch) {
        return new Index2D(patch.id % xSplit, patch.id / xSplit);
    }

    /**
     * @param index
     * @return
     */
    public Patch2D<T> getPatch(int index) {
        return list.get(index);
    }

    /**
     * @param xIndex
     * @param yIndex
     * @return
     */
    public Patch2D<T> getPatch(int xIndex, int yIndex) {
        if (xIndex < 0 || yIndex < 0) {
            return null;
        }
        return list.get(yIndex * xSplit + xIndex);
    }

    /**
     *
     * @param position
     * @return the patch includes the given point, or null if there are no including
     *         patch.
     */
    public Patch2D<T> getPatch(Position2D position) {
        final Position2D size = gridSize();
        final int index = (int) (position.y / size.y) * xSplit + (int) (position.x / size.x);
        return list.get(index);
    }

    /**
     * @param range
     * @return
     */
    public Patch2D<T> getPatch(Range2D range) {
        if (!range.size().equals(gridSize())) {
            return null;
        }
        final Patch2D<T> patch = getPatch(range.leftBottom);
        if (patch == null) {
            return null;
        }
        return (patch.getRange().leftBottom.equals(range.leftBottom)) ? patch : null;
    }

    /**
     * @return
     */
    public Position2D gridSize() {
        return new Position2D(allRange.size().x / xSplit, allRange.size().y / ySplit);
    }

    /**
     * @throws Exception
     */
    public void migrate() throws Exception {
        final List<T> toMigrate = new ArrayList<>();
        // find elements to migrate and remove them.
        forEachPatch((patch) -> {
            final Iterator<T> iter = patch.iterator();
            while (iter.hasNext()) {
                final T t = iter.next();
                if (!patch.getRange().contains(t.position())) {
                    toMigrate.add(t);
                    iter.remove();
                }
            }
        });
        // put removed elements to the next patch.
        toMigrate.forEach((t) -> {
            final Patch2D<T> dest = getPatch(t.position());
            if (dest == null) {
                throw new IndexOutOfBoundsException("Migrate error. " + t + " move to out of range");
            }
            dest.put(t);
        });
    }

    /**
     * @return
     */
    public int numPatches() {
        return list.size();
    }

    /**
     * Assigned equally the patches to threads in order from left bottom and do
     * calculation. It may be uneven when calculations size on each patch is uneven.
     *
     * @param parallelism
     * @param action
     */
    public void parallelForEach(int parallelism, Consumer<T> action) {
        parallelForEachPatch(parallelism, (patch) -> {
            patch.forEach(action);
        });
    }

    /**
     * Assigned equally the patches to threads in order from left bottom and do
     * calculation. It may be uneven when calculations size on each patch is uneven.
     *
     * @param parallelism
     * @param action
     */
    public void parallelForEachPatch(int parallelism, Consumer<Patch2D<T>> action) {
        if (parallelism < 1) {
            throw new IllegalArgumentException();
        }
        finish(() -> {
            final int rem = numPatches() % parallelism;
            final int quo = numPatches() / parallelism;
            final Iterator<Patch2D<T>> pIter = list.values().iterator();

            for (int i = 0; pIter.hasNext(); i++) {
                final int nbPatch = (i < rem) ? quo + 1 : quo;
                final List<Patch2D<T>> splitList = new ArrayList<>();
                for (int j = 0; j < nbPatch; j++) {
                    splitList.add(pIter.next());
                }
                async(() -> {
                    splitList.forEach((patch) -> {
                        action.accept(patch);
                    });
                });
            }
        });
    }

    /**
     * @param position
     * @param t
     */
    public void put(T t) {
        final Patch2D<T> patch = getPatch(t.position());
        if (patch == null) {
            throw new IndexOutOfBoundsException("Couldn't put " + t + ". Out of patch range. ");
        }
        patch.put(t);
    }

    /**
     * Remove the given element from this. WARNING: If you haven't called migrate()
     * after the given element's position has changed,, it may not be removed
     * successfully.
     *
     * @param t
     * @return
     */
    public boolean remove(T t) {
        final Patch2D<T> patch = getPatch(t.position());
        if (patch == null) {
            return false;
        }
        return patch.remove(t);
    }

    /**
     * @return
     */
    public long size() {
        long size = 0;
        // Count all local patches size. Should modify to hold a field variable?
        // The reasons implements former now is that, each patch size is not definite
        // and each patch object may call "put" or "remove", so PatchList can't count
        // such case.
        for (final Entry<Integer, Patch2D<T>> entry : list.entrySet()) {
            size += entry.getValue().size();
        }
        return size;
    }

    /**
     *
     * WARNING: If you haven't called migrate() after the given element's position
     * has changed,, it may not be get successfully.
     *
     * @param range
     * @return
     */
    public Patch2DList<T> subList(Range2D range) {
        // TODO
        return null;
    }

    public Collection<Patch2D<T>> toList() {
        return list.values();
    }

    /**
     * @param x
     * @param edge
     * @return
     */
    public double wrappedX(double x, Edge edge) {
        final double left = allRange.leftBottom.x;
        final double right = allRange.rightTop.x;
        final double size = right - left;
        switch (edge) {
        case loop:
            x = x - left;
            return left + ((x < 0) ? (x % size + size) : (x % size));
        case wall:
            return (x < left) ? left : Math.min(x, right); // FIXME: y should not positioned right.
        }
        throw new UnsupportedOperationException("Not implemented yet about " + edge);
    }

    /**
     * @param xIndex
     * @param edge
     * @return
     */
    public int wrappedX(int xIndex, Edge edge) {
        switch (edge) {
        case loop:
            return (xIndex < 0) ? (xIndex % xSplit + xSplit) : (xIndex % xSplit);
        case wall:
            return (xIndex < 0) ? 0 : Math.min(xIndex, xSplit - 1);
        }
        throw new UnsupportedOperationException("Not implemented yet about " + edge);
    }

    /**
     * @param y
     * @param edge
     * @return
     */
    public double wrappedY(double y, Edge edge) {
        final double bottom = allRange.leftBottom.y;
        final double top = allRange.rightTop.y;
        final double size = top - bottom;
        switch (edge) {
        case loop:
            y = y - bottom;
            return bottom + ((y < 0) ? (y % size + size) : (y % size));
        case wall:
            return (y < bottom) ? bottom : Math.min(y, top); // FIXME: y should not positioned top.
        }
        throw new UnsupportedOperationException("Not implemented yet about " + edge);
    }

    /**
     * @param yIndex
     * @param edge
     * @return
     */
    public int wrappedY(int yIndex, Edge edge) {
        switch (edge) {
        case loop:
            return (yIndex < 0) ? (yIndex % ySplit + ySplit) : (yIndex % ySplit);
        case wall:
            return (yIndex < 0) ? 0 : Math.min(yIndex, ySplit - 1);
        }
        throw new UnsupportedOperationException("Not implemented yet about " + edge);
    }

}
