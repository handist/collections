package handist.util.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import apgas.Place;
import apgas.util.GlobalID;

/**
 * A Map data structure spread over the multiple places.
 * This class allows multiple values for one key.
 */
public class DistMapList<T,U> extends DistMap<T, List<U>> {


    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
            return new AbstractDistCollection.LazyObjectReference<DistMapList<T,U>>(pg1, id1, ()-> {
                return new DistMapList<T,U>(pg1, id1);
        });
    }

    /**
     * Construct a DistMapList.
     */
    public DistMapList() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistMapList with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistMapList(TeamedPlaceGroup placeGroup) {
        super(placeGroup);
    }

    /**
     * Construct a DistMapList with given arguments.
     *
     * @param placeGroup PlaceGroup.
     * @param init the function used in initialization.
     */
    public DistMapList(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id);
    }

    // TODO ...
    //public void setupBranches(DistMap.Generator<T,List<U>> gen)

    /**
     * Put a new value to the list of specified entry.
     *
     * @param key the key of the entry.
     * @param value the new value.
     */
    public boolean put1(T key, U value) {
        List<U> list = data.get(key);
        if (list == null) {
            list = new ArrayList<U>();
            data.put(key, list);
        }
        return list.add(value);
    }

    public boolean putForMove(T key, Collection<U> values) {
        List<U> list = data.get(key);
        if (list == null) {
            list = new ArrayList<U>();
            data.put(key, list);
        }
        // TODO we should check values!=null before transportation
        if (values != null)
            list.addAll(values);
        return false;
    }

    /**
     * Remove the entry corresponding to the specified key.
     *
     * @param key the key corresponding to the value.
     */
    public List<U> removeForMove(T key) {
        List<U> list = data.remove(key);
        return list;
    }

    /**
     * Request that the specified value is put to the list corresponding to the given key when #sync is called.
     *
     * @param key the key of the list.
     * @param pl the destination place.
     * @param mm MoveManagerLocal
     */
    @SuppressWarnings("unchecked")
    public void putAtSync(T key, U value, Place pl, MoveManagerLocal mm) {
        DistMapList<T,U> toBranch = this; // using plh@AbstractCol
        Serializer serialize = (ObjectOutputStream s) -> {
            s.writeObject(key);
            s.writeObject(value);
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            T k = (T)ds.readObject();
            U v = (U)ds.readObject();
            toBranch.put1(k, v);
        };
        mm.request(pl, serialize, deserialize);
    }

    @SuppressWarnings("unchecked")
    public void moveAtSync(T key, Place pl, MoveManagerLocal mm) {
        if (pl.equals(here()))
            return;
        if (!containsKey(key))
            throw new RuntimeException("DistMapList cannot move uncontained entry: " + key);
        final DistMapList<T, U> toBranch = this; // using plh@AbstractCol
        Serializer serialize = (ObjectOutputStream s) -> {
            List<U> value = this.removeForMove(key);
            // TODO we should check values!=null before transportation
            s.writeObject(key);
            s.writeObject(value);
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            T k = (T) ds.readObject();
            // TODO we should check values!=null before transportation
            List<U> v = (List<U>) ds.readObject();
            toBranch.putForMove(k, v);
        };
        mm.request(pl, serialize, deserialize);
    }

    /**
     * Apply the same operation onto the all local entries.
     *
     * @param op the operation.
     */
    public void forEach1(BiConsumer<T, U> op) {
        for (Map.Entry<T, List<U>> entry: data.entrySet()) {
            T key = entry.getKey();
            for (U value: entry.getValue()) {
                op.accept(key, value);
            }
        }
    }

    /**
     * Apply the same operation onto the all elements including other places and create new DistMapList which consists of the results of the operation.
     *
     * @param op the operation.
     * @return a new DistMapList which consists of the result of the operation.
     */
    public <S> DistMapList<T,S> map(BiFunction<T, U, S> op) {
        // TODO
        throw new Error("not implemented yet");
        /*return new DistMapList[T,S](placeGroup, team, () => {
            val dst = new HashMap[T,List[S]]();
            for (entry in data.entries()) {
                val key = entry.getKey();
                val old = entry.getValue();
                val list = new ArrayList[S](old.size());
                for (v in old) {
                    list.add(op(key, v));
                }
                dst(entry.getKey()) = list;
            }
            return dst;
        });*/
    }

    /**
     * Reduce the all local elements using given function.
     *
     * @param op the operation.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    /*
    public def reduceLocal[S](op: (S,U)=>S, unit: S): S {
        var accum: S = unit;
        for (entry in data.entries()) {
            for (value in entry.getValue()) {
                accum = op(accum, value);
            }
        }
        return accum;
    }
    
    def create(placeGroup: PlaceGroup, team: Team, init: ()=>Map[T, List[U]]){
        // return new DistMapList[T,U](placeGroup, init) as AbstractDistCollection[Map[T,List[U]]];
        return null as AbstractDistCollection[Map[T, List[U]]];
    }
    
    public def versioningMapList(srcName : String){
        // return new BranchingManager[DistMapList[T,U], Map[T,List[U]]](srcName, this);
        return null as BranchingManager[DistMapList[T,U], Map[T,List[U]]];
    }*/
    //TODO
    //In the cunnrent implementation of balance(), 
    // DistIdMap treat the number of key as the load of the PE, not using the number of elements in the value lists. 
}
