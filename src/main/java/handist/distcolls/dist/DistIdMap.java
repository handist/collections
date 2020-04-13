package handist.distcolls.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import apgas.Place;
import apgas.util.GlobalID;

/**
 * Distributed Collection like Map[Long,Value].
 */
public class DistIdMap<T> extends DistMap<Long, T>
//TODO
/* implements ManagedDistribution[Long] */
{

    private static int _debug_level = 0;
    transient DistManager.Index ldist;
    transient float[] locality;

    public Map<Long, Place> getDist() { return ldist.dist; }
    Map<Long, Integer> getDiff() { return ldist.diff; }
    public DistributionLong getDistributionLong() { return new DistributionLong(getDist()); }

    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
            return new AbstractDistCollection.LazyObjectReference<DistIdMap<T>>(pg1, id1, ()-> {
                return new DistIdMap<T>(pg1, id1);
        });
    }

    /**
     * Construct a DistIdMap.
     * Place.places() is used as the PlaceGroup of the new instance.
     */
    public DistIdMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistIdMap with the given argument.
     * Team(placeGroup) is used as the PlaceGroup of the new instance.
     *
     * @param placeGroup the PlaceGroup.
     */
    public DistIdMap(TeamedPlaceGroup placeGroup) {
        super(placeGroup);
        //TODO
        this.ldist = new DistManager.Index();
        ldist.setup(data.keySet());
        locality = new float[placeGroup.size()];
        Arrays.fill(locality, 1.0f);
    }

    protected DistIdMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id);
        //TODO
        this.ldist = new DistManager.Index();
        ldist.setup(data.keySet());
        locality = new float[placeGroup.size()];
        Arrays.fill(locality, 1.0f);
    }


    /**
     * Remove the all local entries.
     */
    public void clear() {
        super.clear();
        this.ldist.clear();
        Arrays.fill(locality, 1.0f);
    }

    /*
     * Get the corresponding value of the specified id.
     *
     * @param id a Long type value.
     * @return the corresponding value of the specified id.
     */
    public T get(long id) {
        return data.get(id);
    }

    /*
     * Put a new entry.
     *
     * @param id a Long type value.
     * @param value a value.
     */
    public T put(long id, T value) throws Exception {
        if (data.containsKey(id)) {
            return data.put(id, value);
        }
        ldist.add(id);
        return data.put(id, value);
    }

    private T putForMove(long key, byte mType, T value) throws Exception {
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
        return data.put(key, value);
    }

    public boolean delete(long id) {
        ldist.remove(id);
        return super.delete(id);
    }

    /*
     * Remove the corresponding value of the specified id.
     *
     * @param id a Long type value.
     */
    public T remove(long id) {
        ldist.remove(id);
        return super.remove(id);
    }

    private T removeForMove(long id) {
        return data.remove(id);
    }

    /*
     * Return true if the entry corresponding to the specified id is local.
     *
     * @return true or false.
     */
    public boolean containsId(long id) {
        return super.containsKey(id);
    }

    /*
     * Get a place where the the corresponding entry of the specified id is stored.
     * Return null when it doesn't exist.
     *
     * @param id a Long type value.
     * @return the Place.
     */
    public Place getPlace(long id) {
        return ldist.dist.get(id);
    }

    /*
     * Return the Set of local ids.
     *
     * @return the Set of local ids.
     */
    public Set<Long> idSet() {
        return keySet();
    }

    /**
     * Execute the specified operation with the corresponding value of the specified id.
     * If the entry is stored at local, the operation is executed sequaltially.
     * If the entry is stored at a remote place, the operation is asynchronously executed at the place.
     *
     * @param id a Long type value.
     * @param op the operation.
     */
    public void execAt(long id, SerializableConsumer<T> op) {
        Place place = getPlace(id);
        if (place.equals(here())) {
            op.accept(data.get(id));
            return;
        }
        asyncAt(place, ()->  {
            op.accept(data.get(id));
        });
    }
    @Override
    public void moveAtSync(Long key, Place dest, MoveManagerLocal mm) {
	moveAtSync(key.longValue(), dest, mm);
    }
    
    @SuppressWarnings("unchecked")
    public void moveAtSync(final long key, Place dest, MoveManagerLocal mm) {
        if (dest.equals(here()))
            return;

        final DistIdMap<T> toBranch = this;
        Serializer serialize = (ObjectOutputStream s) -> {
            T value = this.removeForMove(key);
            byte mType = ldist.moveOut(key, dest);
            s.writeLong(key);
            s.writeByte(mType);
            s.writeObject(value);
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            long k = ds.readLong();
            byte mType = ds.readByte();
            T v = (T) ds.readObject();
            if (_debug_level > 5) {
                System.err.println("[" + here() + "] putForMove key: " + k + " keyType: " + mType + " value: " + v);
            }
            toBranch.putForMove(k, mType, v);
        };
        mm.request(dest, serialize, deserialize);
    }
    
    @Override 
    @SuppressWarnings("unchecked")
    public void moveAtSync(Collection<Long> keys, Place dest, MoveManagerLocal mm) {
        if (dest.equals(here())) return;
        final DistIdMap<T> collection = this;
        Serializer serialize = (ObjectOutputStream s) -> {
            int size = keys.size();
            s.writeInt(size);
            for (Long key: keys) {
                T value = collection.removeForMove(key);
                byte mType = ldist.moveOut(key, dest);
                s.writeLong(key);
                s.writeByte(mType);
                s.writeObject(value);
            }
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            int size = ds.readInt();
            for (int i =0; i<size; i++) {
                long key = ds.readLong();
                byte mType = ds.readByte();
                T value = (T)ds.readObject();
                collection.putForMove(key, mType, value);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    @SuppressWarnings("unchecked")
    public void moveAtSyncCount(int count, Place dest, MoveManagerLocal mm) {
        if (dest.equals(here())) return;
        final DistIdMap<T> collection = this;
        Serializer serialize = (ObjectOutputStream s) -> {
            int size = count;
            s.writeInt(size);
            long[] keys = new long[size];
            Object[] values = new Object[size];

            int i = 0;
            for (Map.Entry<Long, T> entry: data.entrySet()) {
                if (i == size) break;
                keys[i] = entry.getKey();
                values[i] = entry.getValue();
                i += 1;
            }
            for (int j=0; j<size; j++) {
                s.writeLong(keys[j]);
            }
            for (int j=0; j<size; j++) {
                s.writeObject(values[j]);
            }
            for (int j=0; j<size; j++) {
                long key = keys[j];
                collection.removeForMove(key);
                byte mType = ldist.moveOut(key, dest);
                s.writeByte(mType);
            }
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            int size = ds.readInt();
            long[] keys = new long[size];
            Object[] values = new Object[size];

            for (int j=0; j<size; j++) {
                keys[j]= ds.readLong();
            }
            for (int j=0; j<size; j++) {
                values[j] = ds.readObject();
            }
            for (int j=0; j<size; j++) {
                byte mType = ds.readByte();
                collection.putForMove(keys[j], mType, (T)values[j]);
            }
        };
        mm.request(dest, serialize, deserialize);
    }
    @Override
    public void moveAtSync(Function<Long, Place> rule, MoveManagerLocal mm) {
        final DistIdMap<T> collection = this;
        HashMap<Place, ArrayList<Long>> keysToMove = new HashMap<>();
        collection.forEach((Long key, T value) -> {
            Place destination = rule.apply(key);
            if (!keysToMove.containsKey(destination)) {
                keysToMove.put(destination, new ArrayList<Long>());
            }
            keysToMove.get(destination).add(key);
        });
        for (Place p: keysToMove.keySet()) {
            moveAtSync(keysToMove.get(p), p, mm);
        }
    }
    @Override
    public void moveAtSync(Distribution<Long> dist, MoveManagerLocal mm) {
        Function<Long,Place> rule = (Long key) -> { return dist.place(key);};
        moveAtSync(rule, mm);
    }

    /* will be implemented in Java using TreeMap
    public def moveAtSync(range: LongRange, place: Place, mm:MoveManagerLocal) {U haszero}: void {

    }
    */
    // TODO???
    //public def moveAtSync(dist:Distribution[LongRange], mm:MoveManagerLocal): void {
    // no need for sparse array


    /**
     * Update the distribution information of the entries.
     */
    public void updateDist() {
        ldist.updateDist(placeGroup);
    }

    /*
    public def versioningIdMap(srcName : String){
        // return new BranchingManager[DistIdMap[T], Map[Long,T]](srcName, this);
        return null as BranchingManager[DistIdMap[T], Map[Long, T]];
    }*/

    /* Ensure calling updateDist() before balance()
     * balance() should be called in all places
     */
    public void checkDistInfo(long[] result) {
        for (Map.Entry<Long, Place> entry : ldist.dist.entrySet()) {
            // val k = entry.getKey();
            Place v = entry.getValue();
            result[placeGroup.rank(v)] += 1;
        }
    }

/*
    //TODO different naming convention of balance methods with DistMap
    public void balance(MoveManagerLocal mm) throws Exception {
        int pgSize = placeGroup.size();
        ArrayList<IFPair> listPlaceLocality = new ArrayList<>();
        float localitySum = 0.0f;
        long globalDataSize =0;
        long[] localDataSize = new long[pgSize];

        for (int i = 0; i<locality.length; i++) {
            localitySum += locality[i];
        }
 

        for (int i=0; i< pgSize; i++) {
            globalDataSize += localDataSize[i];
            float normalizeLocality = locality[i] / localitySum;
            listPlaceLocality.add(new IFPair(i, normalizeLocality));
        }

        listPlaceLocality.sort((IFPair a1, IFPair a2)->{
            return (a1.second < a2.second) ? -1 : (a1.second > a1.second) ? 1 : 0;
        });

        if (_debug_level > 5) {
            for (IFPair pair: listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        IFPair[] cumulativeLocality = new IFPair[pgSize];
        float sumLocality = 0.0f;
        for (int i=0; i<pgSize; i++) {
            sumLocality += listPlaceLocality.get(i).second;
            cumulativeLocality[i] = new IFPair(listPlaceLocality.get(i).first, sumLocality);
        }
        cumulativeLocality[pgSize - 1] = new IFPair(listPlaceLocality.get(pgSize - 1).first, 1.0f);

        if (_debug_level > 5) {
            for (int i=0; i<pgSize; i++) {
                IFPair pair = cumulativeLocality[i];
                System.out.print("(" + pair.first + ", " + pair.second + ", " + localDataSize[pair.first] + "/" + globalDataSize + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        ArrayList<ArrayList<ILPair>> moveList = new ArrayList<>(pgSize); // ArrayList(index of dest Place, num data to export)
        LinkedList<ILPair> stagedData = new LinkedList<>(); // ArrayList(index of src, num data to export)
        long previousCumuNumData = 0;

        for (int i=0; i<pgSize; i++) {
            moveList.add(new ArrayList<ILPair>());
        }

        for (int i=0; i<pgSize; i++) {
            int placeIdx = cumulativeLocality[i].first;
            float placeLocality = cumulativeLocality[i].second;
            long cumuNumData = (long) (((float)globalDataSize) * placeLocality);
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
                    ILPair pair = stagedData.removeFirst();
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

        // Move Data
        for (int i=0; i<pgSize; i++) {
            if (placeGroup.get(i).equals(here())) {

            }
        }

    }

    public void balance(float[] newLocality, MoveManagerLocal mm) throws Exception {
        System.arraycopy(newLocality, 0, locality, 0, placeGroup().size);
        balance(mm);
    }
*/
}
