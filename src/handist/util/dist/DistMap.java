package handist.util.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import apgas.Constructs;
import apgas.Place;

/**
 * A Map data structure spread over the multiple places.
 */
public class DistMap<T,U> extends AbstractDistCollection<Map<T, U>> {
//TODO implements Relocatable

    /**
     * Construct a DistMap.
     */
    public DistMap() {
        this(TeamedPlaceGroup.world);
    }

    @Override
    public Map<T,U> getInitData() {
    	return new HashMap<>();
    }

    /**
     * Construct a DistMap with the given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistMap(TeamedPlaceGroup pg) {
        super(pg, new HashMap<T,U>());
    }

    public static interface Generator<K,V> extends BiConsumer<Place, DistMap<K,V>>, Serializable {
    }

    public static <K,V> DistMap<K,V> make(TeamedPlaceGroup pg, Generator<K, V> gen) {
    	Place from = here();
    	DistMap<K,V> dmap = new DistMap<K,V>(pg);
    	pg.broadcastFlat(()->{
    		gen.accept(here(), dmap);
    	});
    	return dmap;
    }


    /**
     * Remove the all local entries.
     */
    public void clear() {
        this.data.clear();
    }

    /**
     * Return the PlaceGroup.
     *
     * @return the PlaceGroup.
     */
    // public TeamedPlaceGroup placeGroup(): PlaceGroup = placeGroup;

    /**
     * Return the number of the local entries.
     *
     * @return the number of the local entries.
     */
    public int size() {
        return data.size();
    }

    /**
     * Return the value corresponding to the specified key.
     * If the specified entry is not found,
     * return the value of Zero.get[U]().
     *
     * @param key the key corresponding to the value.
     * @return the value corresponding to the specified key.
     */
    public U get(T key) {
        return data.get(key);
    }

    /**
     * Put a new entry.
     *
     * @param key the key of the new entry.
     * @param value the value of the new entry.
     */
    public U put(T key, U value) {
        return data.put(key, value);
    }

    private U putForMove(T key, U value) {
    	if (data.containsKey(key)) {
    		throw new RuntimeException("DistMap cannot override existing entry: " + key);
    	}
    	return data.put(key, value);
    }

    public boolean delete(T key) {
    	U result = data.remove(key);
        return (result!=null);
    }

    /**
     * Remove the entry corresponding to the specified key.
     *
     * @param key the key corresponding to the value.
     */
    public U remove(T key) {
        return data.remove(key);
    }

    /**
     * Apply the same operation onto the all local entries.
     *
     * @param op the operation.
     */
    public void forEach(BiConsumer<T,U> op) {
        data.forEach(op);
    }

    /**
     * Apply the same operation onto the all elements including other place and create a new DistMap which consists of the results of the operation.
     *
     * @param op the operation.
     * @return a DistMap which consists of the results of the operation.
     */
    public <S> DistMap<T,S> map(Function<U,S> op) {
    	// TODO
    	/*
        return new DistMap<T,S>(placeGroup, team, () -> {
            val dst = new HashMap<T,S>();
            for (entry in entries()) {
                val key = entry.getKey();
                val value = entry.getValue();
                dst(key) = op(value);
            }
            return dst;
        });*/
    	return null;
    }

    /**
     * Reduce the all elements including other place using the given operation.
     *
     * @param op the operation.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public U reduce(BiFunction<U,U,U> op, U unit) {
    	return reduce(op, op, unit);
    }

    /**
     * Reduce the all elements including other place using the given operation.
     *
     * @param lop the operation using in the local reduction.
     * @param gop the operation using in the reduction of the results of the local reduction.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public <S> S reduce(BiFunction<S,U,S> lop, BiFunction<S,S,S> gop, S unit) {
    	// TODO
    	throw new Error("Not implemented yet.");
    	/*
        val reducer = new Reducible[S]() {
            public def zero() = unit;
            public operator this(a: S, b: S) = gop(a, b);
        };
        return finish (reducer) {
            placeGroup.broadcastFlat(() => {
                offer(reduceLocal(lop, unit));
            });
        };*/

    }

    /**
     * Reduce the all local elements using the given operation.
     *
     * @param op the operation using in the reduction.
     * @param unit the zero value of the reduction.
     * @return the result of the reduction.
     */
    public <S> S reduceLocal(BiFunction<S,U,S> Fop, S unit) {
    	// TODO may be build-in method for Map
        S accum = unit;
        for (Map.Entry<T, U> entry: data.entrySet()) {
            accum = Fop.apply(accum, entry.getValue());
        }
        return accum;
    }

    /*
     * Return true if the specified entry is exist at local.
     *
     * @param key a key.
     * @return true or false.
     */
    public boolean containsKey(T key) {
        return data.containsKey(key);
    }

    /**
     * Return the Set of local keys.
     *
     * @return the Set of local keys.
     */
    public Set<T> keySet() {
        return data.keySet();
    }

    /**
     * Return the Set of local entries.
     *
     * @return the Set of local entries.
     */
    public Set<Map.Entry<T,U>> entrySet() {
        return data.entrySet();
    }

    /**
     * Request that the specified element is relocated when #sync is called.
     *
     * @param key the key of the relocated entry.
     * @param pl the destination place.
     * @param mm MoveManagerLocal
     */
    public void moveAtSync(T key, Place pl, MoveManagerLocal mm) {
    	if (pl.equals(Constructs.here())) return;
    	final DistMap<T,U> toBranch = this;
        Serializer serialize = (ObjectOutputStream s) -> {
        	U value = this.remove(key);
        	s.writeObject(key);
        	s.writeObject(value);
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            T k = (T)ds.readObject();
            U v = (U)ds.readObject();
            toBranch.putForMove(k, v);
        };
        mm.request(pl, serialize, deserialize);
    }

    public void moveAtSync(List<T> keys, Place pl, MoveManagerLocal mm) {
        if (pl.equals(Constructs.here())) return;
        final DistMap<T,U> collection = this;
        Serializer serialize = (ObjectOutputStream s) -> {
        	int size = keys.size();
        	s.writeInt(size);
        	for (T key: keys) {
        		U value = collection.remove(key);
        		s.writeObject(key);
        		s.writeObject(value);
        	}
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
        	int size = ds.readInt();
        	for (int i =1; i<= size; i++) {
        		T key = (T)ds.readObject();
        		U value = (U)ds.readObject();
        		collection.putForMove(key, value);
        	}
        };
        mm.request(pl, serialize, deserialize);
    }

    public void moveAtSync(Function<T,Place> rule, MoveManagerLocal mm) {
        DistMap<T,U> collection = this;
        HashMap<Place, List<T>> keysToMove = new HashMap<>();
        collection.forEach((T key, U value) -> {
        	Place destination = rule.apply(key);
        	if (!keysToMove.containsKey(destination)) {
        		keysToMove.put(destination, new ArrayList<T>());
        	}
        	keysToMove.get(destination).add(key);
        });
        for (Map.Entry<Place, List<T>> entry: keysToMove.entrySet()) {
        	moveAtSync(entry.getValue(), entry.getKey(), mm);
        }
    }

    public void moveAtSync(Distribution<T> dist, MoveManagerLocal mm) {
    	Function<T,Place> rule = (T key) -> { return dist.place(key);};
    	moveAtSync(rule, mm);
    }

    public void relocate(Function<T,Place> rule, MoveManagerLocal mm) throws Exception {
    	for (T key: data.keySet()) {
    		Place place = rule.apply(key);
    		moveAtSync(key, place, mm);
    	}
    	mm.sync();
    }

    public void relocate(Function<T,Place> rule) throws Exception {
        relocate(rule, new MoveManagerLocal(placeGroup));
    }
    boolean debugPrint() { return true; }

    void teamedBalance() {
        LoadBalancer.MapBalancer<T, U> balance = new LoadBalancer.MapBalancer<>(this.data, placeGroup);
        balance.execute();
        if(debugPrint()) System.out.println(here() + " balance.check1");
        clear();
        if(debugPrint()) {
        	System.out.println(here() + " balance.check2");
        	System.out.println(here() + " balance.ArrayList.size() : " + data.size());
        }
        long time = - System.nanoTime();
        time += System.nanoTime();
        if(debugPrint()) {
//        	System.out.println(here() + " count : " + (count) + " ms");
//        	System.out.println(here() + " put : " + (total/(1000000)) + " ms");
        	System.out.println(here() + " for : " + (time/(1000000)) + " ms");
        	System.out.println(here() + " data.size() : " + size());
        	System.out.println(here() + " balance.check3");
        }

    }

    public void balance() {
        finish(()->{
        	for(Place p: super.placeGroup.places) {
        		asyncAt(p, () -> {
        			this.teamedBalance();
        		});
            }
        });
    }

    public void integrate(Map<T, U> src) {
        for(Map.Entry<T,U> e: src.entrySet()) {
            put(e.getKey(), e.getValue());
        }
    }

/*    Abstractovdef create(placeGroup: PlaceGroup, team: Team, init: ()=>Map[T, U]){
        // return new DistMap[T,U](placeGroup, init) as AbstractDistCollection[Map[T,U]];
        return null as AbstractDistCollection[Map[T,U]];
    }*/
/*
    public def versioningMap(srcName : String){
        // return new BranchingManager[DistMap[T,U], Map[T,U]](srcName, this);
        return null as BranchingManager[DistMap[T,U], Map[T,U]];
    }*/

    public String toString() {
    	StringWriter out0 = new StringWriter();
    	PrintWriter out = new PrintWriter(out0);
        out.println("at "+ here());
        for(Map.Entry<T,U> e : data.entrySet()) {
            out.println("key : "+e.getKey() + ", value : " + e.getValue());
        }
        out.close();
        return out0.toString();
    }

    void printLocalData(){
    	System.out.println(this);
    }

}
