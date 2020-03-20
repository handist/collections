package handist.util.dist;

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
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import apgas.Place;
import apgas.util.GlobalID;
import handist.util.ChunkedList;
import handist.util.LongRange;
import handist.util.RangedList;
import static apgas.Constructs.*;

/**
 * A class for handling objects at multiple places. It is allowed to add new
 * elements dynamically. This class provides the method for load balancing.
 *
 * Note: In the current implementation, there are some limitations.
 *
 * o There is only one load balancing method. The method flattens the number of
 * elements of the all places.
 */
public class DistCol<T> extends AbstractDistCollection /* implements List[T], ManagedDistribution[LongRange] */ {

    private static int _debug_level = 5;

    transient DistManager.Range ldist;
    transient float[] locality;
    public transient ChunkedList<T> data;

    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new AbstractDistCollection.LazyObjectReference<DistCol<T>>(pg1, id1, () -> {
            return new DistCol<T>(pg1, id1);
        });
    }

    /**
     * Create a new DistCol using the given arguments.
     *
     * @param placeGroup an instance of PlaceGroup.
     * @param team       an instance of Team.
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
        locality = initialLocality(placeGroup.size);
    }

    public DistCol(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        super(placeGroup, id);
        this.ldist = new DistManager.Range();
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
    Map<LongRange, Integer> getDiff() { return ldist.diff; }
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
    /*
     * public def containIndex(i: Long): Boolean { return
     * getLocalInternal().data.containIndex(i); }
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

    public void putChunk(final RangedList<T> c) throws Exception {
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
        if (data.removeChunk(c)==null) {
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
        ChunkExtractLeft (final RangedList<T> original, final long splitPoint) {
            this.original = original;
            this.splitPoint = splitPoint;
        }
        List<RangedList<T>>  extract() {
            return original.splitRange(splitPoint);
        }
    }
    static class ChunkExtractMiddle<T> {
        public RangedList<T> original;
        public long splitPoint1;
        public long splitPoint2;
        ChunkExtractMiddle(final RangedList<T> original,final long splitPoint1, final long splitPoint2) {
    	    this.original = original;
	        this.splitPoint1 = splitPoint1;
    	    this.splitPoint2 = splitPoint2;
    	}

        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint1, splitPoint2);
        }
    }

    static class ChunkExtractRight<T>{
        public RangedList<T> original;
        public long splitPoint;
        ChunkExtractRight(final RangedList<T> original,final long splitPoint) {
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
	        if (cRange.begin <= range.begin) {
	            if (cRange.end <= range.begin) {  //cRange.max < range.min) {
		            // skip
		        } else {
		            // range.min <= cRange.max
		            if (cRange.begin == range.begin) {
		                if (cRange.end <= range.end) {
			                // add cRange.min..cRange.max
			                chunksToMove.add(c);
 			            } else {
			                // range.max < cRange.max
			                // split at range.max/range.max+1
			                // add cRange.min..range.max
			                chunksToExtractLeft.add(new ChunkExtractLeft<T>(c, range.end/*max + 1*/));
			            }
		            } else {
		                // cRange.min < range.min
			            if (range.end < cRange.end) {
			                // split at range.min-1/range.min
			                // split at range.max/range.max+1
			                // add range.min..range.max
			                chunksToExtractMiddle.add(new ChunkExtractMiddle<T>(c, range.begin, range.end/*max + 1*/));
			            } else {
			                // split at range.min-1/range.min
			                // cRange.max =< range.max
			                // add range.min..cRange.max
			                chunksToExtractRight.add(new ChunkExtractRight<T>(c, range.begin));
			            }
		            }
		        }
	        } else {
	            // range.min < cRange.min
		        if (range.end <= cRange.begin) { //range.max < cRange.min) {
		            // skip
		        } else {
		            // cRange.min <= range.max
		            if (cRange.end <= range.end) {
		                // add cRange.min..cRange.max
			            chunksToMove.add(c);
		            } else {
		                // split at range.max/range.max+1
			            // add cRange.min..range.max
			            chunksToExtractLeft.add(new ChunkExtractLeft<T>(c, range.end/*max + 1*/));
		            }
		        }
	        }
	    });

	    for (final ChunkExtractLeft<T> chunkToExtractLeft: chunksToExtractLeft) {
	        final RangedList<T> original = chunkToExtractLeft.original;
	        final List<RangedList<T>> splits = chunkToExtractLeft.extract();
//	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
    	    removeChunk(original);
//	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
    	    putChunk(splits.get(0)/*first*/);
//	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
    	    putChunk(splits.get(1)/*second*/);
	        chunksToMove.add(splits.get(0)/*first*/);
	}

	    for (final ChunkExtractMiddle<T> chunkToExtractMiddle: chunksToExtractMiddle) {
    	    final RangedList<T> original = chunkToExtractMiddle.original;
	        final List<RangedList<T>> splits = chunkToExtractMiddle.extract();
//	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
	        removeChunk(original);
//	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
    	    putChunk(splits.get(0)/*first*/);
//	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
	        putChunk(splits.get(1)/*second*/);
//	    System.out.println("[" + here.id + "] putChunk " + splits.third.getRange());
    	    putChunk(splits.get(2)/*third*/);
	        chunksToMove.add(splits.get(1)/*second*/);
	    }

	    for (final ChunkExtractRight<T> chunkToExtractRight: chunksToExtractRight) {
    	    final RangedList<T> original = chunkToExtractRight.original;
	        final List<RangedList<T>> splits = chunkToExtractRight.extract();
//	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
    	    removeChunk(original);
//	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
    	    putChunk(splits.get(0)/*first*/);
//	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
	        putChunk(splits.get(1)/*second*/);
	        chunksToMove.add(splits.get(1)/*second*/);
	    }

	    moveAtSync(chunksToMove, dest, mm);
    }

    public void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManagerLocal mm) throws Exception {
        if (_debug_level > 5) {
            System.out.print("[" + here().id + "] moveAtSync List[RangedList[T]]: ");
	        for (final RangedList<T> rl: cs) {
    	        System.out.print("" + rl.getRange() + ", ");
	        }
	        System.out.println(" dest: " + dest.id);
        }

        if(dest.equals(here())) return;

    	final DistCol<T> toBranch = this; // using plh@AbstractCol
        final Serializer serialize = (ObjectOutputStream s) -> {
	        final ArrayList<Byte> keyTypeList = new ArrayList<>();
    	    for(final RangedList<T> c: cs) {
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
	        for(final RangedList<T> c: chunks) {
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
    static class Pair<F,S> {
        F first; S second;
        Pair(F first, S second) {
            this.first = first;
            this.second = second;
        }
    }

    public void moveAtSyncCount(final ArrayList<ILPair> moveList, final MoveManagerLocal mm) throws Exception {
        // TODO ->LinkedList? sort??
        final ArrayList<LongRange> localKeys = new ArrayList<>();
        localKeys.addAll(ranges());
        localKeys.sort((LongRange range1, LongRange range2) -> {
            long len1 = range1.end - range1.begin;
            long len2 = range2.end - range2.begin;
            return (int)(len1 - len2);
        });
        if (_debug_level > 5) {
            System.out.print("[" + here() + "] ");
            for (int i=0; i<localKeys.size(); i++) {
                System.out.print("" + localKeys.get(i).begin + ".." + localKeys.get(i).end + ", ");
            }
            System.out.println();
        }
        for (final ILPair moveinfo: moveList) {
            final long count = moveinfo.second;
            final Place dest = placeGroup.get(moveinfo.first);
            if (_debug_level > 5) {
                System.out.println("[" + here() + "] move count=" + count + " to dest " + dest.id);
            }
            if (dest.equals(here())) continue;
            long sizeToSend = count;
            while (sizeToSend > 0) {
                final LongRange lk = localKeys.remove(0);
                final long len = lk.end - lk.begin;
                if (len > sizeToSend) {
                    moveAtSync(new LongRange(lk.begin, lk.begin + sizeToSend), dest, mm);
                    localKeys.add(0, new LongRange(lk.begin + sizeToSend, lk.end));
                    break;
                } else {
                    moveAtSync(lk, dest, mm);
                    sizeToSend -= len;
                }
            }
        }
    }

    public void moveAtSync(Function<LongRange, List<Pair<Place,LongRange>>> rule, MoveManagerLocal mm) throws Exception {
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
	    for (final Place place: rangesToMove.keySet()) {
    	    for (final LongRange range: rangesToMove.get(place)) {
	            moveAtSync(range, place, mm);
	        }
	    }
    }

    public void moveAtSync(final RangedDistribution<LongRange> dist, final MoveManagerLocal mm) throws Exception {
        moveAtSync((LongRange range) -> { return dist.placeRanges(range);}, mm);
    }

    public void moveAtSync(final Distribution<Long> dist, final MoveManagerLocal mm) throws Exception {
        moveAtSync((LongRange range) -> {
	        ArrayList<Pair<Place,LongRange>> listPlaceRange = new ArrayList<>();
	        for (final Long key: range) {
    	        listPlaceRange.add(new Pair<Place, LongRange>(dist.place(key), new LongRange(key, key+1)));
	        }
	        return listPlaceRange;
	    }, mm);
    }

    /*
     * public def moveAtSync(dist:RangedDistribution[LongRange],
     * mm:MoveManagerLocal) { // moveAtSync(range, ..) を複数回呼ぶか //
     * あるいは、moveAtSync(List[LongRange],) を作成して、それを一回呼ぶか。 // いずれにせよ、closure の中身を
     * private method に分離して、少し掃除したほうがいいかも？ for(place in placeGroup) { val ranges =
     * dist.ranges(place); for(range in ranges) moveAtSync(range, place, mm); } }
     */

    /*
     * public def relocate(dist:RangedDistribution) { val mm = new
     * MoveManagerLocal(placeGroup,team); moveAtSync(dist, mm); mm.sync(); }
     */

    public void updateDist() {
        ldist.updateDist(placeGroup);
    }

    static class IFPair {
        int first;
        float second;

        public IFPair(int first, float second) {
            this.first = first;
            this.second = second;
        }
    }
     static class ILPair {
        int first; long second;
        public ILPair(int first, long second){
            this.first = first; this.second = second;
        }
    }

    /*
     * Ensure calling updateDist() before balance() balance() should be called in
     * all places
     */
    public void balance(MoveManagerLocal mm) throws Exception {
        final int pgSize = placeGroup.size();
        final IFPair[] listPlaceLocality = new IFPair[pgSize];
        float localitySum = 0.0f;
        long globalDataSize = 0;
        final long[] localDataSize = new long[pgSize];

        for (int i=0; i<pgSize; i++) {
            localitySum += locality[i];
        }
        for (final Map.Entry<LongRange,Place> entry: ldist.dist.entrySet()) {
            final LongRange k = entry.getKey();
            final Place p = entry.getValue();
            localDataSize[placeGroup.rank(p)] += k.size();
        }

        for (int i=0; i<pgSize; i++) {
            globalDataSize += localDataSize[i];
            final float normalizeLocality = locality[i] / localitySum;
            listPlaceLocality[i] = new IFPair(i, normalizeLocality);
        }
        Arrays.sort(listPlaceLocality, (IFPair a1, IFPair a2) -> { return Float.compare(a1.second, a2.second); });

        if (_debug_level > 5) {
            for (IFPair pair: listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        IFPair[] cumulativeLocality = new IFPair[pgSize];
        float sumLocality = 0.0f;
        for (int i=0; i< pgSize; i++) {
            sumLocality += listPlaceLocality[i].second;
            cumulativeLocality[i] = new IFPair(listPlaceLocality[i].first, sumLocality);
        }
        cumulativeLocality[pgSize - 1] = new IFPair(listPlaceLocality[pgSize - 1].first, 1.0f);

        if (_debug_level > 5) {
            for (int i=0; i<pgSize; i++) {
                IFPair pair = cumulativeLocality[i];
                System.out.print("(" + pair.first + ", " + pair.second + ", " + localDataSize[pair.first] + "/" + globalDataSize + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        final ArrayList<ArrayList<ILPair>>  moveList = new ArrayList<>(pgSize); // ArrayList(index of dest Place, num data to export)
        ArrayList<ILPair> stagedData = new ArrayList<>(); // ArrayList(index of src, num data to export)
        long previousCumuNumData = 0;

        for (int i=0; i<pgSize; i++) {
            moveList.add(new ArrayList<ILPair>());
        }

        for (int i=0; i<pgSize; i++) {
            int placeIdx = cumulativeLocality[i].first;
            float placeLocality = cumulativeLocality[i].second;
            long cumuNumData = (long)(((float)globalDataSize) * placeLocality);
            long targetNumData = cumuNumData - previousCumuNumData;
            if (localDataSize[placeIdx] > targetNumData) {
                stagedData.add(new ILPair(placeIdx, localDataSize[placeIdx] - targetNumData));
                if (_debug_level > 5) {
                    System.out.print("stage src: " + placeIdx + " num: " + (localDataSize[placeIdx] - targetNumData) + ", ");
                }
            }
            previousCumuNumData = cumuNumData;
        }
        if (_debug_level > 5) {
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        previousCumuNumData = 0;
        for (int i=0; i<pgSize; i++) {
            int placeIdx = cumulativeLocality[i].first;
            float placeLocality = cumulativeLocality[i].second;
            long cumuNumData = (long)(((float)globalDataSize) * placeLocality);
            long targetNumData = cumuNumData - previousCumuNumData;
            if (targetNumData > localDataSize[placeIdx]) {
                long numToImport = targetNumData - localDataSize[placeIdx];
                while (numToImport > 0) {
                    ILPair pair = stagedData.remove(0);
                    if (pair.second > numToImport) {
                        moveList.get(pair.first).add(new ILPair(placeIdx, numToImport));
                        stagedData.add(new ILPair(pair.first, pair.second - numToImport));
                        numToImport = 0;
                    } else {
                        moveList.get(pair.first).add(new ILPair(placeIdx, pair.second));
                        numToImport -= pair.second;
                    }
                }
            }
            previousCumuNumData = cumuNumData;
        }

        if (_debug_level > 5) {
            for (int i=0; i<pgSize; i++) {
                for (ILPair pair: moveList.get(i)) {
                    System.out.print("src: " + i + " dest: " + pair.first + " size: " + pair.second + ", ");
                }
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }


        if (_debug_level > 5) {
            long[] diffNumData = new long[pgSize];
            for (int i=0; i<pgSize; i++) {
                for (ILPair pair: moveList.get(i)) {
                    diffNumData[i] -= pair.second;
                    diffNumData[pair.first] += pair.second;
                }
            }
            for (IFPair pair: listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ", " + (localDataSize[pair.first] + diffNumData[pair.first]) + "/" + globalDataSize + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }


        for (int i=0; i<pgSize; i++) {
            if (placeGroup.get(i).equals(here())) {
                moveAtSyncCount(moveList.get(i), mm);
            }
        }
    }

    public void balance(final float[] newLocality, final MoveManagerLocal mm) throws Exception {
        //Rail.copy[Float](newLocality, locality);        
        if(newLocality.length!=placeGroup.size()) throw new RuntimeException("[DistCol] the size of newLocality must be the same with placeGroup.size()");
        System.arraycopy(newLocality, 0, locality, 0, locality.length);
        balance(mm);
    }

    public void balance() {
        // new LoadBalancer[T](data, placeGroup, team).execute();
        throw new UnsupportedOperationException();
    }

    // TODO
    public Collection<LongRange> ranges() {
        return data.ranges();
    }

    // TODO
    /*
    public <U> void forEach(Pool pool, ReceiverHolder<U> receiverHolder, int nth, BiConsumer<T, Receiver<U>> op) {
        if (isEmpty()) {
            return;
        }
        ParallelAccumulator.execute(pool, getLocalInternal().data, receiverHolder, nth, op);
    }
    
    public <U> Condition asyncForEach(Pool pool, ReceiverHolder<U> receiverHolder, int nth, BiConsumer<T, Receiver<U>> op) {
        if (isEmpty()) {
            final val condition = new Condition();
            condition.release();
            return condition;
        }
        if (_debug_level > 5n) {
    	    System.out.println("DistCol#asyncEach@ " + here + " data:" + data.ranges());
        }
        return ParallelAccumulator.executeAsync(pool, getLocalInternal().data, receiverHolder, nth, op);
    }
    */
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

    /*
    public boolean addAllWhere(final Collection<T> c:Container[T],p:(T)=>Boolean):Boolean {
        throw new UnsupportedOperationException();
    }

    public def removeAllWhere(p:(T)=>Boolean):Boolean {
        throw new UnsupportedOperationException();
    }

    public def removeAt(final i0:Long):T {
        throw new UnsupportedOperationException();
    }

    public def removeFirst():T {
        throw new UnsupportedOperationException();
    }

    public def removeLast():T {
        throw new UnsupportedOperationException();
    }

    public def indexOf(final v:T):Long {
        throw new UnsupportedOperationException();
    }

    public def lastIndexOf(final v:T):Long {
        throw new UnsupportedOperationException();
    }

    public def indexOf(final index:Long,v:T):Long {
        throw new UnsupportedOperationException();
    }

    public def lastIndexOf(final index:Long,v:T):Long {
        throw new UnsupportedOperationException();
    }

    public def iteratorFrom(final index:Long):ListIterator[T] {
        throw new UnsupportedOperationException();
    }

    public def subList(final fromIndex:Long,toIndex:Long):List[T] {
        throw new UnsupportedOperationException();
    }

    public def getFirst():T {
        throw new UnsupportedOperationException();
    }

    public def getLast():T {
        throw new UnsupportedOperationException();
    }

    public def sort() {
        throw new UnsupportedOperationException();
    }

    public def sort(cmp:(T,T)=>Int) { throw new UnsupportedOperationException();} 
    */
}
