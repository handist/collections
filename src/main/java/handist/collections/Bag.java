/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import handist.collections.dist.DistBag;

import static apgas.Constructs.*;

/**
 * Container for user-defined types. 
 * <p>
 * This class implements the {@link ParallelReceiver} interface and as such, is 
 * capable of concurrently receiving instances of type T from multiple threads. 
 * Its use is therefore recommended in parallel implementations of 
 * {@code forEach} methods.
 *
 * @param <T> type of the object handled
 */
public class Bag<T> extends AbstractCollection<T> implements  ParallelReceiver<T>, Serializable {

	/**
	 * Iterator class for {@link Bag}
	 */
	private class It implements Iterator<T> {
		/** Iterator on the contents of a List<T> */
		Iterator<T> cIter;
		/** Iterator on {@link Bag#bags}, iterates on lists of T */
		Iterator<List<T>> oIter;

		/**
		 * Constructor. Initializes the two iterators used to iterate on the
		 * lists contained in {@link Bag#bags} and the iterator on these lists. 
		 */
		public It() {
			oIter = bags.iterator();
			if (oIter.hasNext()) {
				cIter = oIter.next().iterator();
			} else {
				cIter = null;
			}
		}

		@Override
		public boolean hasNext() {
			if (cIter == null) {
				return false;
			}
			while(true) {
				if (cIter.hasNext()) {
					return true;
				}
				if (oIter.hasNext()) {
					cIter = oIter.next().iterator();
				} else {
					cIter = null;
					return false;
				}
			}
		}

		@Override
		public T next() {
			if (hasNext()) {
				return cIter.next();
			}
			throw new IndexOutOfBoundsException();
		}

	}

	/** Serial Version UID */
	private static final long serialVersionUID = 5436363137856754303L;

	/**
	 * Container of type T. The instances are split in multiple lists, one for
	 * each of the calls made to {@link #getReceiver()}. 
	 */
	ConcurrentLinkedDeque<List<T>> bags;

	/**
	 * Default constructor. 
	 */
	public Bag() {
		bags = new ConcurrentLinkedDeque<>();
	}

	/**
	 * Constructor to create a Bag with the same contents as another {@link Bag}
	 * or {@link DistBag}.
	 * @param bag the bag to copy
	 */
	public Bag(Bag<T> bag) {
		bags = bag.bags;
	}

	/**
	 * Adds a list of instances to this instance.
	 * 
	 * @param l list of T
	 */
	public void addBag(List<T> l) {
		bags.add(l);
	}

	/**
	 * Adds a list of instances to this instance.
	 *
	 * @param bag Bag of T
	 */
	public void addBag(Bag<T> bag) {
		bags.addAll(bag.bags);
	}

	/**
	 * Removes all contents from this bag. 
	 */
	@Override
	public void clear() {
		bags.clear();
	}

	/**
	 * Copies this {@link Bag}. The individual T elements contained in this 
	 * instance are not cloned, both this instance and the returned instance 
	 * will share the same objects.  
	 */
	@Override
	public Bag<T> clone() {
		Bag<T> result = new Bag<T>();
		for (Collection<T> bag : bags) {
			ArrayList<T> nbag = new ArrayList<>(bag);
			result.addBag(nbag);
		}
		return result;
	}

	@Override
	public boolean contains(Object v) {
		for(Collection<T> bag: bags) {
			if(bag.contains(v)) return true;
		}
		return false;
	}

	/**
	 * Converts the bag into a list and clears the bag.
	 * @return the contents of this instance as a list
	 */
	public List<T> convertToList() {
		// TODO: prepare a smarter implementation
		ArrayList<T> result = new ArrayList<>(this.size());
		for (List<T> c : bags) {
			result.addAll(c);
		}
		bags.clear();
		return result;
	}

	@Override
	public void forEach(final Consumer<? super T> action) {
		bags.forEach((Collection<T> bag) -> {
			bag.forEach(action);
		});
	}

	/**
	 * Launches a parallel forEach on the elements of this collection. The
	 * elements contained in the individual lists (created either through 
	 * {@link #addBag(List)} or {@link #getReceiver()}) are submitted to the 
	 * provided {@link ExecutorService}. This method than waits for the 
	 * completion of the tasks to return.   
	 * 
	 * @param pool the executor service in charge of processing this instance
	 * @param action the action to perform on individual elements
	 */
	public void forEach(ExecutorService pool, final Consumer<? super T> action) {
		List<Future<?>> futures = forEachConst(pool, action);
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				throw new ParallelExecutionException("[Bag] exception raised by worker threads.", e);
			}
		}
	}
	
	private List<Future<?>> forEachConst(ExecutorService pool, final Consumer<? super T> action) {
		ArrayList<Future<?>> futures = new ArrayList<>();
		for (Collection<T> bag : bags) {
			futures.add(pool.submit(() -> {
				bag.forEach(action);
			}));
		}
		return futures;
	}
	
	/**
	 * Launches a parallel forEach on the elements of this collection. The
	 * elements contained in the individual lists (created either through 
	 * {@link #addBag(List)} or {@link #getReceiver()}) are submitted to the 
	 * provided {@link ExecutorService}. This method than waits for the 
	 * completion of the tasks to return.   
	 * 
	 * @param action the action to perform on individual elements
	 */
	public void parallelForEach(final Consumer<? super T> action) {
		finish(() -> {
			forEachParallelBody((List<T> sub) -> {
				sub.forEach(action);
			});
		});
	}
	
	private void forEachParallelBody(final Consumer<List<T>> run) { 
		Bag<T> separated = this.separate(Runtime.getRuntime().availableProcessors() * 2);
		for(List<T> sub: separated.bags) {
			async(() -> {
				run.accept(sub);
			});
		}
	}

	/**
	 * Separates the contents of the Bag in <em>n</em> parts.
	 * This can be used to apply a forEach method in parallel using 'n' threads
	 * for instance. The method returns Bag containing <em>n</em> {@link List}.
	 *
	 * @param n the number of parts in which to split the Bag
	 * @return {@link Bag} containing the same number of
	 * 	elements {@link List}s
	 */
	public Bag<T> separate(int n){
		int totalNum = this.size();
		int rem = totalNum % n;
		int quo = totalNum / n;
		Bag<T> result = new Bag<T>();
		Iterator<T> it = this.iterator();
		
		for(int i = 0; i < n; i++) {
			List<T> r = new ArrayList<T>();
			result.addBag(r);
			int rest = quo + ((i < rem)? 1 : 0);
			while(rest > 0) {
				r.add(it.next());
				rest--;
			}
		}
		return result;
	}
	
	/**
	 * Adds a new list to this instance and returns a {@link Consumer} which 
	 * will place the T instances it receives into this dedicated list.
	 */
	@Override
	public Consumer<T> getReceiver() {
		final ArrayList<T> bag = new ArrayList<>();
		bags.add(bag);
		return new Consumer<T>() {
			@Override
			public void accept(T t) {
				bag.add(t);
			}
		};
	}

	@Override
	public boolean isEmpty() {
		for (Collection<T> bag : bags) {
			if (!bag.isEmpty())
				return false;
		}
		return true;
	}

	/**
	 * Returns an iterator over the elements contained in this instance
	 */
	@Override
	public Iterator<T> iterator() {
		return new It();
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		int size = in.readInt();
		bags = new ConcurrentLinkedDeque<>();
		ArrayList<T> bag1 = new ArrayList<T>(size);
		for (int i = 0; i < size; i++) {
			bag1.add((T) in.readObject());
		}
		bags.add(bag1);
	}

	/**
	 * Removes one element contained in this instance and returns it. If there 
	 * are no elements in this instance, returns {@code null}. 
	 * 
	 * @return the element removed from this collection, or {@code null} if this
	 * 	instance does not contain anything 
	 */
	public synchronized T remove() {
		while (true) {
			if (bags.isEmpty())
				return null;
			List<T> bag = bags.getLast();
			if (bag.isEmpty()) {
				bags.removeLast();
			} else {
				return bag.remove(bag.size() - 1);
			}
		}
	}

	/**
	 * Removes and returns {@code n} elements from this instance into a list. If 
	 * there are less than {@code n} elements contained in this instance, 
	 * removes and returns all the elements. If this instance is empty, returns 
	 * an empty list. 
	 * @param n the number of elements to remove from this instance
	 * @return a list containing at most {@code n} elements
	 */
	public synchronized List<T> remove(int n) {
		ArrayList<T> result = new ArrayList<T>(n);
		while(n > 0) {
			if(bags.isEmpty())	return result;
			List<T> bag = bags.getLast();
			if(bag.isEmpty()) {
				bags.removeLast();
				continue;
			}
			result.add(bag.remove(bag.size() - 1)); 
			n--;
		}        
		return result;
	}

	@Override
	public int size() {
		int size = 0;
		for (Collection<T> bag : bags)
			size += bag.size();
		return size;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[Bag]");
		for (Collection<T> bag: bags) {
			sb.append(bag.toString()+ ":");
		}
		sb.append("end of Bag");
		return sb.toString();
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		// System.out.println("writeChunk:"+this);
		out.writeInt(size());
		for (Collection<T> bag : bags) {
			for (T item : bag) {
				out.writeObject(item);
			}
		}
	}
}
