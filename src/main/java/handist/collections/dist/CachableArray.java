package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.BiConsumer;
import java.util.function.Function;

import apgas.Place;
import apgas.util.PlaceLocalObject;
import mpi.MPIException;

/**
 * A class for handling objects using the Master-Proxy mechanism. The master
 * place has the body of each elements. The proxy places have the branch of each
 * elements.
 *
 * Note: In the current implementation, there are some limitations.
 *
 * o The first place of the PlaceGroup is selected as the master place
 * automatically. o To add any new elements is not allowed. The elements are
 * assigned only in the construction.
 */
public class CachableArray<T> extends PlaceLocalObject implements List<T> {
    protected transient ArrayList<T> data;
    public transient TeamedPlaceGroup placeGroup;
    public transient Place master;

    /**
     * Create a new CacheableArray using the given list. data must not be shared
     * with others.
     *
     * @param data
     * @param placeGroup
     * @param master
     */
    protected CachableArray(TeamedPlaceGroup placeGroup, Place master, ArrayList<T> data) {
        this.data = data;
        this.placeGroup = placeGroup;
        this.master = master;
    }

    /**
     * Create a new CacheableArray using the given arguments. The elements of new
     * CacheableArray and given collection is the same. The proxies are also set in
     * the construction.
     *
     * @param placeGroup a PlaceGroup.
     * @param team       a Team.
     * @param indexed    an instance of Indexed that is used for initializing the
     *                   elements.
     */
    public static <T> CachableArray<T> make(final TeamedPlaceGroup pg, List<T> data) {
        final Place master = here();
        final ArrayList<T> body = new ArrayList<>();
        body.addAll(data);
        return PlaceLocalObject.make(pg.places(), () -> new CachableArray<T>(pg, master, body));
    }

    /**
     * Return the PlaceGroup.
     */
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    /**
     * Broadcast from master place to proxy place. Packing elements using the
     * specified function. It is assumed that type U is declared as struct and it
     * has no references.
     *
     * Note: Now, this method is implemented in too simple way.
     *
     * @param team   a Team used in broadcast the packed data.
     * @param pack   a function which packs the elements of master node.
     * @param unpack a function which unpacks the received data and inserts the
     *               unpacked data to each proxy.
     */
    @SuppressWarnings("unchecked")
    public <U> void broadcast(Function<T, U> pack, BiConsumer<T, U> unpack) {
        Serializer serProcess = (ObjectOutputStream ser) -> {
            for (T elem : data) {
                ser.writeObject(pack.apply(elem));
            }
        };
        DeSerializer desProcess = (ObjectInputStream des) -> {
            for (T elem : data) {
                U diff = (U) des.readObject();
                unpack.accept(elem, diff);
            }
        };
        try {
            CollectiveRelocator.bcastSer(placeGroup, master, serProcess, desProcess);
        } catch (MPIException e) {
            e.printStackTrace();
            throw new Error("[CachableArray] MPIException raised.");
        }
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        Iterator<T> ei = this.data.iterator();
        sb.append("CacheableArray[");
        while (true) {
            if (ei.hasNext()) {
                sb.append(ei.next());
            } else {
                break;
            }
            if (ei.hasNext()) {
                sb.append(" ");
            }
        }

        sb.append("]");
        return sb.toString();
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return data.contains(o);
    }

    @Override
    public Iterator<T> iterator() {
        return Collections.unmodifiableList(data).iterator();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException("[CachableArray] No direct access to members is allowed.");
    }

    @Override
    public <S> S[] toArray(S[] a) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public boolean add(T e) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return data.containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public T get(int index) {
        return data.get(index);
    }

    @Override
    public T set(int index, T element) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public void add(int index, T element) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public T remove(int index) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public int indexOf(Object o) {
        return data.indexOf(o);
    }

    @Override
    public int lastIndexOf(Object o) {
        return data.lastIndexOf(o);
    }

    @Override
    public ListIterator<T> listIterator() {
        return Collections.unmodifiableList(data).listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return Collections.unmodifiableList(data).listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

}
