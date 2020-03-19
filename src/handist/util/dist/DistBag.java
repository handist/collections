package handist.util.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.ListIterator;
import java.util.function.BiConsumer;

import apgas.Place;
import apgas.util.GlobalID;

/**
 * A class for handling objects at multiple places.
 * It is allowed to add new elements dynamically.
 * This class provides the method for load balancing.
 *
 * Note: In the current implementation, there are some limitations.
 *
 * o There is only one load balancing method.
 *   The method flattens the number of elements of the all places.
 */
public class DistBag<T> extends AbstractDistCollection /* implements Container[T], ReceiverHolder[T] */{

    transient public ArrayList<T> data;

    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new AbstractDistCollection.LazyObjectReference<DistBag<T>>(pg1, id1, ()-> {
            return new DistBag<T>(pg1, id1);
        });
    }

/*
    @TransientInitExpr(getReceiversInternal())
    transient val receivers: Rail[DistBagReceiver[T]];

    private final def getReceiversInternal(): Rail[DistBagReceiver[T]] {
        val local = getLocal[DistBagLocal[T]]();
        if (local == null) {
            return null;
        }
        return local.receivers;
    }
*/

    /**
     * Create a new DistBag.
     * Place.places() is used as the PlaceGroup.
     */
    public DistBag() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Create a new DistBag using the given arguments.
     *
     * @param placeGroup a PlaceGroup.
     * @param team a Team.
     */
    public DistBag(TeamedPlaceGroup placeGroup) {
        super(placeGroup);
        this.data = new ArrayList<T>();
        //TODO
        // receiver?
    }
    protected DistBag(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id);
        this.data = new ArrayList<T>();
        //TODO
        // receiver?
    }

    public static interface Generator<V> extends BiConsumer<Place, DistBag<V>>, Serializable {
    }

    // TODO ...
    public void setupBranches(Generator<T> gen) {
        final DistBag<T> handle = this;
        finish(()->{
            placeGroup.broadcastFlat(()->{
                gen.accept(here(), handle);
            });
        });
    }

    /**
     * Add new element.
     *
     * @param v a new element.
     */
    public boolean add(T v) {
        return data.add(v);
    }

    /**
     * Add all elements int the given list.
     *
     * @param list the list.
     */
    public boolean addAll(Collection<T> list) {
        return this.data.addAll(list);
    }

    /**
     * Remove a element at the local storage.
     */
    public T remove() {
        return data.remove(data.size()-1);
    }

    /**
     * Clear the local elements.
     */
    public void clear() {
        data.clear();
    }

    /**
     * Return whether DistBag's local storage has no value.
     *
     * @return true if DistBag's local storage has no value.
     */
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Return whether the DistBag contains the given value.
     * This method uses the T#equals to evaluate the equality.
     *
     * @param v a value of type T.
     * @return true or false.
     */
    public boolean contains(T v) {
        return data.contains(v);
    }

    /**
     * Return whether the DistBag contains all the values of the given Container.
     * This method used the T#equals to evaluate the equality.
     *
     * @param container a Container[T].
     * @return true if DistBag contains the all given values.
     */
    public boolean containsAll(Collection<T> container) {
        return data.containsAll(container);
    }

    /**
     * Return the number of local elements.
     *
     * @return the number of local elements.
     */
    public int size() {
        return data.size();
    }

    /**
     * Return a Container that has the same values of DistBag's local storage.
     *
     * @return a Container that has the same values of local storage.
     */
    /*
    public Collection<T> clone(): Container[T] {
        return data.clone();
    }*/

    /**
     * Return the iterator for the local elements.
     *
     * @return the iterator.
     */
    public ListIterator<T> iterator() {
        return this.data.listIterator();
    }
/*
    public def getReceiver(): Receiver[T] {
        val id = Runtime.workerId();
        if (receivers(id) == null) {
            receivers(id) = new DistBagReceiver[T](this);
        }
        return receivers(id);
    }
*/

    /**
     * gather all place-local elements to the root Place.
     *
     * @param root the place where the result of reduction is stored.
     */
    public void gather(Place root) {
        Serializer serProcess = (ObjectOutputStream ser) -> {
            ser.writeObject(this.data);
        };
        DeSerializerUsingPlace desProcess = (ObjectInputStream des, Place place) -> {
            Collection<T> imported = (Collection<T>) des.readObject();
            this.data.addAll(imported);
        };
        CollectiveRelocator.gatherSer(placeGroup, root, serProcess, desProcess);
        if (!here().equals(root)) {
            clear();
        }
    }

    public void balance() {
        // new LoadBalancer[T](data, placeGroup, team).execute();
        throw new UnsupportedOperationException();
    }
    /*
    public def integrate(src : List[T]) {
        // addAll(src);
        throw new UnsupportedOperationException();
    }*/

    /*
    public def versioning(srcName : String){
        return new BranchingManager[DistBag[T], List[T]](srcName, this);
    }
    */
/*
    static class DistBagLocal[T] extends Local[List[T]] {

        def this(placeGroup: PlaceGroup, team: Team, data: List[T]) {
            super(placeGroup, team, data);
        }

        val receivers: Rail[DistBagReceiver[T]] = new Rail[DistBagReceiver[T]](Runtime.MAX_THREADS as Long);
    }
*/
/*
    static class DistBagReceiver[T] implements Receiver[T] {

        val distBag: DistBag[T];
        val buffer: List[T] = new ArrayList[T]();

        def this(distBag: DistBag[T]) {
            this.distBag = distBag;
        }

        public def receive(value: T): void {
            buffer.add(value);
        }

        public def close(): void {
            distBag.lock();
            distBag.data.addAll(buffer);
            buffer.clear();
            distBag.unlock();
        }
    }
    */
}
