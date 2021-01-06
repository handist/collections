/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static apgas.Constructs.*;

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
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;
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
 * 
 * @param <T> type of the elements handled by the {@link CachableArray}
 */

public class CachableArray<T> extends PlaceLocalObject implements List<T>, SerializableWithReplace {
    /**
     * Create a new CacheableArray using the given arguments. The elements of the 
     * newly created CacheableArray and the given collection will be identical. 
     * The proxies are also prepared as part of the initialization.
     * 
     * @param <T> type of the elements contained in the instance to create
     * @param pg {@link TeamedPlaceGroup} on which the {@link CachableArray} will be prepared
     * @param data initial content to be placed in the {@link CachableArray}
     * @return the handle to the local instance of the {@link CachableArray}
     */
    
    public static <T> CachableArray<T> make(final TeamedPlaceGroup pg, List<T> data) {
        final Place master = here();
        final ArrayList<T> body = new ArrayList<>();
        body.addAll(data);
        return PlaceLocalObject.make(pg.places(), () -> new CachableArray<T>(pg, master, body));
    }
    protected transient ArrayList<T> data;
    public transient Place master;
    
    public transient TeamedPlaceGroup placeGroup;
    
    /**
     * Create a new CacheableArray using the given list. data must not be shared
     * with others.
     *
     * @param placeGroup group of hosts suceptible to manipulate this instance
     * @param master the "master" of the Cachable array
     * @param data the initial data to create this CachableArray with
     */
    protected CachableArray(TeamedPlaceGroup placeGroup, Place master, ArrayList<T> data) {
        this.data = data;
        this.placeGroup = placeGroup;
	this.master = master;
    }
    
    @Override
    public void add(int index, T element) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }
    
    @Override
    public boolean add(T e) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }
    
    @Override
    public boolean addAll(Collection<? extends T> c) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
        }
    
    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }
    
    /**
     * Broadcast from master place to proxy place, packing elements using the
     * specified function. It is assumed that the type U is declared as a struct
     * and that it does not contain any reference.
     * <p>
     * Note: Currently, this method is implemented in too simple way.
     *
     * @param <U> type used to represent the elements of this instance and that 
     *      is going to be transfered to remote hosts. In the implementation of the
     *      <em>pack</em> and <code>unpack</code>, the programmer may choose to use
     *      T, but this allows any other custom type to be used.
     * @param pack   a function which packs the elements of the master node.
     * @param unpack a function which unpacks the received data and inserts the
     *               unpacked data into the instance local to each proxy.
     */
    @SuppressWarnings("unchecked")
    public <U> void broadcast(Function<T, U> pack, BiConsumer<T, U> unpack) {
	Serializer serProcess = (ObjectOutput s) -> {
	    for (T elem : data) {
	    	s.writeObject(pack.apply(elem));
	    }
	};
	DeSerializer desProcess = (ObjectInput ds) -> {
	    for (T elem : data) {
	    	U diff = (U) ds.readObject();
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

    @Override
    public void clear() {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }
    
    @Override
    public boolean contains(Object o) {
	return data.contains(o);
    }
    
    @Override
    public boolean containsAll(Collection<?> c) {
	return data.containsAll(c);
    }
    
    @Override
    public T get(int index) {
	return data.get(index);
    }
    
    @Override
    public int indexOf(Object o) {
	return data.indexOf(o);
    }
    
    @Override
    public boolean isEmpty() {
	return data.isEmpty();
    }
    
    @Override
    public Iterator<T> iterator() {
	return Collections.unmodifiableList(data).iterator();
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
    
    /**
     * Return the PlaceGroup on which this instance was created.
     * @return the {@link TeamedPlaceGroup} on which this instance was replicated
     */
    public TeamedPlaceGroup placeGroup() {
	return placeGroup;
    }
    
    @Override
    public T remove(int index) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }
    
    @Override
    public boolean remove(Object o) {
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
    public T set(int index, T element) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public int size() {
	return data.size();
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
    }

    @Override
    public Object[] toArray() {
	throw new UnsupportedOperationException("[CachableArray] No direct access to members is allowed.");
    }

    @Override
    public <S> S[] toArray(S[] a) {
	throw new UnsupportedOperationException("[CachableArray] No modification of members is allowed.");
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

}
