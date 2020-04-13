package handist.distcolls.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import handist.distcolls.Bag;
import mpi.MPI;
import mpi.MPIException;

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
    private static int _debug_level = 5;
    transient public Bag<T> data;

    /**
     * Return the number of local elements.
     *
     * @return the number of local elements.
     */
    public int size() {
        return data.size();
    }

    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new AbstractDistCollection.LazyObjectReference<DistBag<T>>(pg1, id1, ()-> {
            return new DistBag<T>(pg1, id1);
        });
    }

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
        this.data = new Bag<T>();
    }
    protected DistBag(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id);
        this.data = new Bag<T>();
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
        return data.remove();
    }

    public Collection<T> removeN(int count) {
        return data.removeN(count);
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
    public Iterator<T> iterator() {
        return this.data.iterator();
    }

    public Consumer<T> getReceiver() {
        return data.getReceiver();
    }


    /**
     * gather all place-local elements to the root Place.
     *
     * @param root the place where the result of reduction is stored.
     */
    @SuppressWarnings("unchecked")
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



    /*
    public def integrate(src : List[T]) {
        // addAll(src);
        throw new UnsupportedOperationException();
    }*/

    public void checkDistInfo(long[] result) {
        TeamedPlaceGroup pg = this.placeGroup;
        long localSize = size(); // int->long
        long[] sendbuf = new long[]{ localSize };
        // team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
        try {
            pg.comm.Allgather(sendbuf, 0, 1, MPI.LONG, result, 0, 1, MPI.LONG);
        } catch (MPIException e) {
            e.printStackTrace();
            throw new Error("[DistMap] network error in balance()");
        }
    }
    @SuppressWarnings("unchecked")
    public void moveAtSyncCount(final int count, Place pl, MoveManagerLocal mm) {
        if (pl.equals(Constructs.here()))
            return;
        final DistBag<T> collection = this;
        Serializer serialize = (ObjectOutputStream s) -> {
            s.writeObject(this.removeN(count));
        };
        DeSerializer deserialize = (ObjectInputStream ds) -> {
            Collection<T> imported = (Collection<T>) ds.readObject();
            collection.addAll(imported);
        };
        mm.request(pl, serialize, deserialize);
    }

    protected void moveAtSyncCount(final ArrayList<ILPair> moveList, final MoveManagerLocal mm) throws Exception {
        for (ILPair pair : moveList) {
            if (_debug_level > 5) {
                System.out.println("MOVE src: " + here() + " dest: " + pair.first + " size: " + pair.second);
            }
            if (pair.second > Integer.MAX_VALUE)
                throw new Error("One place cannot receive so much elements: " + pair.second);
            moveAtSyncCount((int) pair.second, placeGroup.get(pair.first), mm);
        }
    }
    /*
    public def versioning(srcName : String){
        return new BranchingManager[DistBag[T], List[T]](srcName, this);
    }
    */

}
