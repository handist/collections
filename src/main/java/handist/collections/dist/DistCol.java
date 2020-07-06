package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.ChunkedList;
import handist.collections.LongRange;
import handist.collections.MultiReceiver;
import handist.collections.RangedList;
import handist.collections.function.LongTBiConsumer;

/**
 * A class for handling objects at multiple places. It is allowed to add new
 * elements dynamically. This class provides the method for load balancing.
 * <p>
 * Note: In the current implementation, there are some limitations.
 * <ul>
 *  <li>There is only one load balancing method: the method flattens the number of
 * elements of the all places.
 * </ul>
 *
 * @param <T> the type of elements handled by this {@link DistCol}
 */
public class DistCol<T> extends AbstractDistCollection /* implements List[T], ManagedDistribution[LongRange] */ {

    private static int _debug_level = 5;

    transient DistManager.Range ldist;
    public transient ChunkedList<T> data;

    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new AbstractDistCollection.LazyObjectReference<DistCol<T>>(pg1, id1, () -> {
            return new DistCol<T>(pg1, id1);
        });
    }

    /**
     * Create a new DistCol. All the hosts participating in the distributed
     * computation are susceptible to handle the created instance. This
     * constructor is equivalent to calling {@link #DistCol(TeamedPlaceGroup)}
     * with {@link TeamedPlaceGroup#getWorld()} as argument.
     */
    public DistCol() {
        this(TeamedPlaceGroup.getWorld());
    }

    private static float[] initialLocality(final int size) {
        final float[] result = new float[size];
        Arrays.fill(result, 1.0f);
        return result;
    }

    public DistCol(final TeamedPlaceGroup placeGroup) {
        super(placeGroup);
        this.ldist = new DistManager.Range();
        this.data = new ChunkedList<T>();
        locality = initialLocality(placeGroup.size);
    }

    public DistCol(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        super(placeGroup, id);
        this.ldist = new DistManager.Range();
        this.data = new ChunkedList<T>();
        locality = initialLocality(placeGroup.size);
    }

    public static interface Generator<V> extends BiConsumer<Place, DistCol<V>>, Serializable {
    }

    // TODO ...
    public void setupBranches(final Generator<T> gen) {
        final DistCol<T> handle = this;
        finish(() -> {
            handle.placeGroup.broadcastFlat(() -> {
                gen.accept(here(), handle);
            });
        });
    }

    public HashMap<LongRange, Place> getDist() {
        return ldist.dist;
    }

    Map<LongRange, Integer> getDiff() {
        return ldist.diff;
    }

    public RangedDistributionLong getRangedDistributionLong() {
        return new RangedDistributionLong(getDist());
    }

    public DistributionLong getDistributionLong() {
        return DistributionLong.convert(getDist());
    }

    /*
    // var proxy:(Long)=>T = null;

    public def setProxy(proxy:(Long)=>T) {
    this.proxy = proxy;
    }
    */

    public int size() {
        return data.size();
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public boolean contains(final T v) {
        return data.contains(v);
    }

    public boolean containsChunk(RangedList<T> c) {
        return data.containsChunk(c);
    }

    public boolean containsIndex(long i) {
        return data.containsIndex(i);
    }

    public boolean containsAll(final Collection<T> vs) {
        return data.containsAll(vs);
    }
    /*
     * public def clone(): DistCol[T] { throw new
     * UnsupportedOperationException("DistCol does not support clone because it is missleading."
     * ); }
     */

    public Iterator<T> iterator() {
        return data.iterator();
    }

    public void clear() {
        data.clear();
        ldist.clear();
        Arrays.fill(locality, 1.0f);
    }

    public void addChunk(final RangedList<T> c) throws Exception {
        ldist.add(c.getRange());
        data.addChunk(c);
    }

    private void putForMove(final RangedList<T> c, final byte mType) throws Exception {
        final LongRange key = c.getRange();
        switch (mType) {
            case DistManager.MOVE_NEW:
                ldist.moveInNew(key);
                break;
            case DistManager.MOVE_OLD:
                ldist.moveInOld(key);
                break;
            default:
                throw new Exception("SystemError when calling putForMove " + key);
        }
        data.addChunk(c);
    }

    public void removeChunk(final RangedList<T> c) throws Exception {
        ldist.remove(c.getRange());
        data.removeChunk(c);
    }

    private void removeForMove(final RangedList<T> c) {
        if (data.removeChunk(c) == null) {
            throw new RuntimeException("DistCol#removeForMove");
        }
    }
    /*
     * public void integrate(ChunkedList<T> c) { //TODO throw new
     * UnsupportedOperationException(); }
     */
    /*
     * def create(placeGroup: PlaceGroup, team: Team, init: ()=>ChunkedList[T]){
     * //TODO return null as AbstractDistCollection[ChunkedList[T]]; }
     */

    static class ChunkExtractLeft<T> {
        public RangedList<T> original;
        public long splitPoint;

        ChunkExtractLeft(final RangedList<T> original, final long splitPoint) {
            this.original = original;
            this.splitPoint = splitPoint;
        }

        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint);
        }
    }

    static class ChunkExtractMiddle<T> {
        public RangedList<T> original;
        public long splitPoint1;
        public long splitPoint2;

        ChunkExtractMiddle(final RangedList<T> original, final long splitPoint1, final long splitPoint2) {
            this.original = original;
            this.splitPoint1 = splitPoint1;
            this.splitPoint2 = splitPoint2;
        }

        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint1, splitPoint2);
        }
    }

    static class ChunkExtractRight<T> {
        public RangedList<T> original;
        public long splitPoint;

        ChunkExtractRight(final RangedList<T> original, final long splitPoint) {
            this.original = original;
            this.splitPoint = splitPoint;
        }

        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint);
        }
    }

    public void moveAtSync(final LongRange range, final Place dest, final MoveManagerLocal mm) throws Exception {
        if (_debug_level > 5) {
            System.out.println("[" + here().id + "] moveAtSync range: " + range + " dest: " + dest.id);
        }
        final ArrayList<RangedList<T>> chunksToMove = new ArrayList<>();
        final ArrayList<ChunkExtractLeft<T>> chunksToExtractLeft = new ArrayList<>();
        final ArrayList<ChunkExtractMiddle<T>> chunksToExtractMiddle = new ArrayList<>();
        final ArrayList<ChunkExtractRight<T>> chunksToExtractRight = new ArrayList<>();
        data.forEachChunk((RangedList<T> c) -> {
            final LongRange cRange = c.getRange();
            if (cRange.from <= range.from) {
                if (cRange.to <= range.from) { //cRange.max < range.min) {
                    // skip
                } else {
                    // range.min <= cRange.max
                    if (cRange.from == range.from) {
                        if (cRange.to <= range.to) {
                            // add cRange.min..cRange.max
                            chunksToMove.add(c);
                        } else {
                            // range.max < cRange.max
                            // split at range.max/range.max+1
                            // add cRange.min..range.max
                            chunksToExtractLeft.add(new ChunkExtractLeft<T>(c, range.to/*max + 1*/));
                        }
                    } else {
                        // cRange.min < range.min
                        if (range.to < cRange.to) {
                            // split at range.min-1/range.min
                            // split at range.max/range.max+1
                            // add range.min..range.max
                            chunksToExtractMiddle.add(new ChunkExtractMiddle<T>(c, range.from, range.to/*max + 1*/));
                        } else {
                            // split at range.min-1/range.min
                            // cRange.max =< range.max
                            // add range.min..cRange.max
                            chunksToExtractRight.add(new ChunkExtractRight<T>(c, range.from));
                        }
                    }
                }
            } else {
                // range.min < cRange.min
                if (range.to <= cRange.from) { //range.max < cRange.min) {
                    // skip
                } else {
                    // cRange.min <= range.max
                    if (cRange.to <= range.to) {
                        // add cRange.min..cRange.max
                        chunksToMove.add(c);
                    } else {
                        // split at range.max/range.max+1
                        // add cRange.min..range.max
                        chunksToExtractLeft.add(new ChunkExtractLeft<T>(c, range.to/*max + 1*/));
                    }
                }
            }
        });

        for (final ChunkExtractLeft<T> chunkToExtractLeft : chunksToExtractLeft) {
            final RangedList<T> original = chunkToExtractLeft.original;
            final List<RangedList<T>> splits = chunkToExtractLeft.extract();
            //	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
            removeChunk(original);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
            addChunk(splits.get(0)/*first*/);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
            addChunk(splits.get(1)/*second*/);
            chunksToMove.add(splits.get(0)/*first*/);
        }

        for (final ChunkExtractMiddle<T> chunkToExtractMiddle : chunksToExtractMiddle) {
            final RangedList<T> original = chunkToExtractMiddle.original;
            final List<RangedList<T>> splits = chunkToExtractMiddle.extract();
            //	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
            removeChunk(original);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
            addChunk(splits.get(0)/*first*/);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
            addChunk(splits.get(1)/*second*/);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.third.getRange());
            addChunk(splits.get(2)/*third*/);
            chunksToMove.add(splits.get(1)/*second*/);
        }

        for (final ChunkExtractRight<T> chunkToExtractRight : chunksToExtractRight) {
            final RangedList<T> original = chunkToExtractRight.original;
            final List<RangedList<T>> splits = chunkToExtractRight.extract();
            //	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
            removeChunk(original);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
            addChunk(splits.get(0)/*first*/);
            //	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
            addChunk(splits.get(1)/*second*/);
            chunksToMove.add(splits.get(1)/*second*/);
        }

        moveAtSync(chunksToMove, dest, mm);
    }
    @SuppressWarnings("unchecked")
    public void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManagerLocal mm) throws Exception {
        if (_debug_level > 5) {
            System.out.print("[" + here().id + "] moveAtSync List[RangedList[T]]: ");
            for (final RangedList<T> rl : cs) {
                System.out.print("" + rl.getRange() + ", ");
            }
            System.out.println(" dest: " + dest.id);
        }

        if (dest.equals(here()))
            return;

        final DistCol<T> toBranch = this; // using plh@AbstractCol
        final Serializer serialize = (ObjectOutputStream s) -> {
            final ArrayList<Byte> keyTypeList = new ArrayList<>();
            for (final RangedList<T> c : cs) {
                keyTypeList.add(ldist.moveOut(c.getRange(), dest));
                this.removeForMove(c);
            }
            s.writeObject(keyTypeList);
            s.writeObject(cs);
        };
        final DeSerializer deserialize = (ObjectInputStream ds) -> {
            final List<Byte> keyTypeList = (List<Byte>) ds.readObject();
            final Iterator<Byte> keyTypeListIt = keyTypeList.iterator();
            final List<RangedList<T>> chunks = (List<RangedList<T>>) ds.readObject();
            for (final RangedList<T> c : chunks) {
                final byte keyType = keyTypeListIt.next();
                final LongRange key = c.getRange();
                if (_debug_level > 5) {
                    System.out.println("[" + here() + "] putForMove key: " + key + " keyType: " + keyType);
                }
                toBranch.putForMove(c, keyType);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    static class Pair<F, S> {
        F first;
        S second;

        Pair(F first, S second) {
            this.first = first;
            this.second = second;
        }
    }

    protected void moveAtSyncCount(final ArrayList<ILPair> moveList, final MoveManagerLocal mm) throws Exception {
        // TODO ->LinkedList? sort??
        final ArrayList<LongRange> localKeys = new ArrayList<>();
        localKeys.addAll(ranges());
        localKeys.sort((LongRange range1, LongRange range2) -> {
            long len1 = range1.to - range1.from;
            long len2 = range2.to - range2.from;
            return (int) (len1 - len2);
        });
        if (_debug_level > 5) {
            System.out.print("[" + here() + "] ");
            for (int i = 0; i < localKeys.size(); i++) {
                System.out.print("" + localKeys.get(i).from + ".." + localKeys.get(i).to + ", ");
            }
            System.out.println();
        }
        for (final ILPair moveinfo : moveList) {
            final long count = moveinfo.second;
            final Place dest = placeGroup.get(moveinfo.first);
            if (_debug_level > 5) {
                System.out.println("[" + here() + "] move count=" + count + " to dest " + dest.id);
            }
            if (dest.equals(here()))
                continue;
            long sizeToSend = count;
            while (sizeToSend > 0) {
                final LongRange lk = localKeys.remove(0);
                final long len = lk.to - lk.from;
                if (len > sizeToSend) {
                    moveAtSync(new LongRange(lk.from, lk.from + sizeToSend), dest, mm);
                    localKeys.add(0, new LongRange(lk.from + sizeToSend, lk.to));
                    break;
                } else {
                    moveAtSync(lk, dest, mm);
                    sizeToSend -= len;
                }
            }
        }
    }

    public void moveAtSync(Function<LongRange, List<Pair<Place, LongRange>>> rule, MoveManagerLocal mm)
            throws Exception {
        final DistCol<T> collection = this;
        final HashMap<Place, ArrayList<LongRange>> rangesToMove = new HashMap<>();

        collection.forEachChunk((RangedList<T> c) -> {
            final List<Pair<Place, LongRange>> destinationList = rule.apply(c.getRange());
            for (final Pair<Place, LongRange> destination : destinationList) {
                final Place destinationPlace = destination.first;
                final LongRange destinationRange = destination.second;
                if (!rangesToMove.containsKey(destinationPlace)) {
                    rangesToMove.put(destinationPlace, new ArrayList<LongRange>());

                }
                rangesToMove.get(destinationPlace).add(destinationRange);
            }
        });
        for (final Place place : rangesToMove.keySet()) {
            for (final LongRange range : rangesToMove.get(place)) {
                moveAtSync(range, place, mm);
            }
        }
    }

    public T get(long i) {
        return data.get(i);
    }

    public long longSize() {
        return data.longSize();
    }

    public int numChunks() {
        return data.numChunks();
    }

    public T set(long i, T value) {
        return data.set(i, value);
    }

    public void moveAtSync(final RangedDistribution<LongRange> dist, final MoveManagerLocal mm) throws Exception {
        moveAtSync((LongRange range) -> {
            return dist.placeRanges(range);
        }, mm);
    }

    public void moveAtSync(final Distribution<Long> dist, final MoveManagerLocal mm) throws Exception {
        moveAtSync((LongRange range) -> {
            ArrayList<Pair<Place, LongRange>> listPlaceRange = new ArrayList<>();
            for (final Long key : range) {
                listPlaceRange.add(new Pair<Place, LongRange>(dist.place(key), new LongRange(key, key + 1)));
            }
            return listPlaceRange;
        }, mm);
    }

    @Override
    public void checkDistInfo(long[] result) {
        for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
            final LongRange k = entry.getKey();
            final Place p = entry.getValue();
            result[placeGroup.rank(p)] += k.size();
        }
    }

    /*
     * public def relocate(dist:RangedDistribution) { val mm = new
     * MoveManagerLocal(placeGroup,team); moveAtSync(dist, mm); mm.sync(); }
     */

    public void updateDist() {
        ldist.updateDist(placeGroup);
    }

    // TODO
    public Collection<LongRange> ranges() {
        return data.ranges();
    }


    public Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads, Consumer<? super T> action) {
        return data.asyncForEach(pool, nthreads, action);
    }

    public <U> Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads,
            BiConsumer<? super T, Consumer<U>> action, MultiReceiver<U> toStore) {
        return data.asyncForEach(pool, nthreads, action, toStore);
    }

    public Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads, LongTBiConsumer<? super T> action) {
        return data.asyncForEach(pool, nthreads, action);
    }

    public <U> void forEach(BiConsumer<? super T, Consumer<U>> action, Collection<? super U> toStore) {
        data.forEach(action, toStore);
    }

    public <U> void forEach(BiConsumer<? super T, Consumer<U>> action, Consumer<U> receiver) {
        data.forEach(action, receiver);
    }

    public void forEach(Consumer<? super T> action) {
        data.forEach(action);
    }

    public <U> void forEach(ExecutorService pool, int nthreads, BiConsumer<? super T, Consumer<U>> action,
            MultiReceiver<U> toStore) {
        data.forEach(pool, nthreads, action, toStore);
    }

    public void forEach(ExecutorService pool, int nthreads, Consumer<? super T> action) {
        data.forEach(pool, nthreads, action);
    }

    public void forEach(ExecutorService pool, int nthreads, LongTBiConsumer<? super T> action) {
        data.forEach(pool, nthreads, action);
    }

    public void forEach(LongTBiConsumer<? super T> action) {
        data.forEach(action);
    }

    public void forEachChunk(Consumer<RangedList<T>> op) {
        data.forEachChunk(op);
    }

    public List<RangedList<T>> filterChunk(final Predicate<RangedList<? super T>> op) {
        return data.filterChunk(op);
    }

    public List<Long> indices() {
        throw new UnsupportedOperationException();
    }

    public void reverse() {
        throw new UnsupportedOperationException();
    }

    public boolean add(final T v) {
        throw new UnsupportedOperationException();
    }

    public void addBefore(final long i, final T v) {
        throw new UnsupportedOperationException();
    }

    public boolean addAll(final Collection<T> elems) {
        throw new UnsupportedOperationException();
    }

    public boolean remove(final T v) {
        throw new UnsupportedOperationException();
    }

    public boolean retainAll(final Collection<T> vs) {
        throw new UnsupportedOperationException();
    }

    public boolean removeAll(final Collection<T> vs) {
        throw new UnsupportedOperationException();
    }
}
