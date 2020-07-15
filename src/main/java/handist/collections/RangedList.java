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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import handist.collections.function.LongTBiConsumer;

/**
 * Interface describing a list defined on long indices. Entries can be defined 
 * on any index contained within the {@link LongRange} used to initialize the
 * collection.
 *  
 * @param <T> type handled by the collection
 */
public interface RangedList<T> extends Iterable<T> {

	public static boolean equals(RangedList<?> rlist1, Object o) {
		if(o==null) return (rlist1==null);
		if(!(o instanceof RangedList)) return false;
		RangedList<?> rlist2 = (RangedList<?>)o;
		// TODO this version is too slow,
		// setupFrom will be the good candidate for fast simul scanner.
		if(!rlist1.getRange().equals(rlist2.getRange())) return false;
		for(long index: rlist1.getRange()) {
			if(!rlist1.get(index).equals(rlist2.get(index))) return false;
		}
		return true;
	}

	public static int hashCode(RangedList<?> rlist) {
		int hashCode = 1;
		// code from JavaAPI doc of List
		for(Object o: rlist) {
			hashCode = 31*hashCode + (o==null ? 0 : o.hashCode());
		}
		return hashCode;
	}

	/**
	 * Returns a copy of this instance, restricted to the contents that are 
	 * included in the specified range.  
	 * @param range portion of the {@link RangedList} to copy
	 * @return a new RangedList which contains the entries of this instance on
	 * 	provided range
	 */
	RangedList<T> cloneRange(LongRange range);

	/**
	 * Indicates if this list contains the provided object. More formally if the
	 * list contains at least one object {@code a} such that 
	 * <code>(a == null) ? o == null : a.equals(o);</code> is true.  
	 * 
	 * @param o the object whose presence is to be checked
	 * @return {@code true} if the collection contains {@code o}, {@code false}
	 * 	otherwise
	 */
	abstract boolean contains(Object o);

	/**
	 * Performs the provided action on each element contained by this instance,
	 * and potentially collect/extract some information into the provided 
	 * receiver. 
	 * <p>
	 * The BiConsumer is applied on each element contained in the collection 
	 * (first parameter of the BiConsumer) with the receiver provided as second 
	 * parameter of this method as the second parameter of the BiConsumer. This 
	 * allows you to make modifications to individual elements and potentially
	 * extract some information (of type U) and store it in the receiver 
	 * provided as second parameter. 
	 * <p>
	 * If you do not need to extract any information from the elements contained
	 * in this instance, you should use {@link #forEach(Consumer)} instead. 
	 * 
	 * @param <U> type of the collected instances
	 * @param action action to perform on each element, potentially  
	 * @param receiver collector of information extracted 
	 */
	default public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, Consumer<? super U> receiver) {
		forEach(getRange(), action, receiver);
	}

	/**
	 * Performs the provided action on every element in the collection
	 */
	default public void forEach(Consumer<? super T> action) {
		forEach(getRange(), action);
	}

	/**
	 * Performs the provided action on elements contained by this instance,
	 * and potentially collect/extract some information into the provided 
	 * receiver. This method has the same effect as 
	 * {@link #forEach(BiConsumer, Consumer)} but its application is restricted
	 * to the range specified as first parameter. 
	 * <p>
	 * The BiConsumer is applied on each element contained in the collection 
	 * (first parameter of the BiConsumer) with the receiver provided as second 
	 * parameter of this method as the second parameter of the BiConsumer. This 
	 * allows you to make modifications to individual elements and potentially
	 * extract some information (of type U) and store it in the receiver 
	 * provided as second parameter. 
	 * <p>
	 * If you do not need to extract any information from the elements contained
	 * in this instance, you should use {@link #forEach(LongRange, Consumer)} 
	 * instead. 
	 *
	 * @param <U> type of the collected instances
	 * @param range range on which the action is to be applied
	 * @param action action to perform on each element, potentially  
	 * @param receiver collector of information extracted 
	 * @see #forEach(LongRange, BiConsumer, Consumer)
	 */
	default public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
			Consumer<? super U> receiver) {
		for (long i = range.from; i < range.to; i++) {
			action.accept(get(i), receiver);
		}   
	}

	/**
	 * Applies the specified action on the elements of this collection that are
	 * present in the specified range. 
	 * This method is similar to {@link #forEach(Consumer)} but the application
	 * of the specified action is restricted to the range specified as first
	 * parameter
	 * @param range range of application of the action
	 * @param action action to perform on individual elements
	 */
	default public void forEach(LongRange range, Consumer<? super T> action) {
		for (long i = range.from; i < range.to; i++) {
			action.accept(get(i));
		}   
	}

	/**
	 * Applies the given action on the index/value pairs present in the 
	 * specified range.
	 * <p>
	 * This method is almost identical to {@link #forEach(LongTBiConsumer)} but 
	 * its application is restricted to the range of indices specified as 
	 * parameter. 
	 * @param range range of indices on which to apply the action
	 * @param action action to perform taking a long and a T as parameter
	 */
	default public void forEach(LongRange range, LongTBiConsumer<? super T> action) {
		for (long i = range.from; i < range.to; i++) {
			action.accept(i, get(i));
		}   
	}

	/**
	 * Performs the specified action on every index/value pair contained in this
	 * collection
	 * @param action action to perform taking a long and a T as parameter
	 */
	default public void forEach(LongTBiConsumer<? super T> action) {
		forEach(getRange(), action);
	}

	/**
	 * Returns the value associated with the provided {@code long} index. 
	 * @param index index of the value to return. 
	 * @return the value associated with this index
	 */
	T get(long index);

	/**
	 * Obtain the {@link LongRange} on which this instance is defined.
	 * @return the {@link LongRange} object representing the 
	 */
	LongRange getRange();

	/**
	 * Indicates if this RangedList is empty, i.e. if it cannot contain any 
	 * entry because it is defined on an empty {@link LongRange}. 
	 * @return {@code true} is the instance is defined on an empty 
	 * {@link LongRange}, {@code false} otherwise. 
	 */
	default public boolean isEmpty() {
		return getRange().size() == 0;
	}

	/**
	 * Returns an iterator that starts from the specified index
	 * @param i starting index of the iterator
	 * @return an Iterator on the elements of this {@link RangedList} 
	 */
	public Iterator<T> iteratorFrom(long i);

	/**
	 * Returns the number of entries in this collection as a {@code long}
	 * @return size of the collection
	 */
	long longSize();

	/**
	 * Creates a new collection from the elements contained in this instance by
	 * transforming them into a new type
	 * @param <U> type of the collection to create
	 * @param func function that returns a type U from the provided T
	 * @return a newly created collection which contains the mapping of the
	 *  elements contained by this instance to type U 
	 */
	default public <U> RangedList<U> map(Function<? super T, ? extends U> func) {
		Chunk<U> result = new Chunk<>(this.getRange());
		result.setupFrom(this, func);
		return result;
	}

	/**
	 * Creates a new collection from the elements contained in this instance on
	 * the specified range by transforming them into a different type
	 * @param <U> type of the collection to create
	 * @param range the range on which to apply the method
	 * @param func function that returns a type U from the provided T
	 * @return a newly created collection which contains the mapping of the
	 *  elements contained by this instance (restricted to the specified range) 
	 *  to type U 
	 */
	default public <U> RangedList<U> map(LongRange range, Function<? super T, ? extends U> func) {
		return this.subList(range.from, range.to).map(func);
	}

	default public void rangeCheck(long target) {
		if(!this.getRange().contains(target)) {
			throw new IndexOutOfBoundsException(
					"[RangedList] range mismatch: " + this.getRange() + " does not include " + target);
		}
	}

	/**
	 * Checks if the provided {@link LongRange} is included in the range of this
	 * instance.
	 * @param target LongRange whose inclusion in this instance is to be checked
	 * @throws ArrayIndexOutOfBoundsException if the provided {@link LongRange}
	 * 	is not included in this instance 
	 */
	default public void rangeCheck(LongRange target) {
		if(!this.getRange().contains(target)) {
			throw new ArrayIndexOutOfBoundsException(
					"[Chunk] range mismatch:" + this.getRange() + " must include " + target);
		}
	}

	/**
	 * Sets the provided value at the specified index
	 * @param index index at which the value should be stored
	 * @param value value to store at the specified index
	 * @return previous value that was stored at this index, {@code null} if
	 * there was no previous value or the previous value stored was {@code null}
	 */
	T set(long index, T value);

	/**
	 * Initializes the values in this instance by applying the provided function
	 * on the elements contained in {@code source}
	 * @param <S> the type handled by the {@link RangedList} given as parameter, 
	 * input for the function
	 * @param source {@link RangedList} instance from which entried for this 
	 *  instance will be extracted
	 * @param func function that takes an object of type S as parameter and
	 * 	returns a type T
	 */
	abstract public <S> void setupFrom(RangedList<S> source, Function<? super S, ? extends T> func);

	/**
	 * Separates this instance into multiple {@link RangedList}s using the 
	 * points given as parameter.
	 * <p>
	 * For instance, if this instance is defined on a range [a,b) and points l, 
	 * m, and n are given as parameter, this method will return 4 {@link RangedList}
	 * defined on [a,l), [l,m), [m,n), and [n,b). 
	 * <p>
	 * The user will be careful to sort the points given as parameter in 
	 * ascending order. Exceptions during the creation of {@link RangedList} 
	 * will be thrown otherwise. 
	 * @param splitPoints the points at which this instance needs to be cut
	 * @return this instance entries split into several {@link RangedList} 
	 */
	default public List<RangedList<T>> splitRange(long ... splitPoints) {
		ArrayList<RangedList<T>> toReturn = new ArrayList<>(splitPoints.length + 1);
		LongRange range = getRange();
		long start = range.from;
		for (long split : splitPoints) {
			toReturn.add(new RangedListView<T>(this, new LongRange(start, split)));
			start = split;
		}
		toReturn.add(new RangedListView<T>(this, new LongRange(start, range.to)));
		return toReturn;
	};

	/**
	 * Provides a RangedList of the elements contained in this instance from 
	 * index <em>begin</em> to index <em>end</em>. 
	 * <p>
	 * If the provided range exceeds the indices contained in this instance
	 * (i.e. if <em>begin</em> is lower than the lowest index contained in this 
	 * instance, or if <em>end</em> is higher than the highest index contained in
	 * this instance) the method will return the elements it contains that fit
	 * within the provided range.
	 * 
	 * @param begin starting index of the desired sub-list
	 * @param end last index of the desired sub-list (exlusive)
	 * @return a ranged list of the elements contained in this 
	 * 	{@link RangedList} that fit in the provided range. 
	 * @throws IllegalArgumentException if <em>begin</em> is superior to 
	 * <em>end</em>.
	 * @throws IndexOutOfBoundsException if the provided range 
	 * has no intersection with the range of this instance. 
	 */
	default public RangedList<T> subList(long begin, long end) {
		if(begin > end) {
			throw new IllegalArgumentException("Cannot obtain a sublist from " +
					begin + " to " + end);
		}
		long from = Math.max(begin, getRange().from);
		long to = Math.min(end, getRange().to);
		if(from>to) throw new IndexOutOfBoundsException("[RangedList] no intersection with ["+ begin +","+end+")");
		LongRange newRange = new LongRange(from, to);
		if (newRange.equals(getRange())) {
			return this;
		}
		return new RangedListView<T>(this, newRange);
	}
	/**
	 * Provides a RangedList of the elements contained in this instance on the 
	 * specified {@link LongRange}.
	 * <p>
	 * If the provided range exceeds the indices contained in this instance
	 * (i.e. if lower bound of the {@link LongRange} is lower than the lowest 
	 * index contained in this instance, or if the upper bound of the provided 
	 * {@link LongRange} is higher than the highest index contained in
	 * this instance) the method will return the elements it contains that fit
	 * within the provided range.
	 * 
	 * @param range range of indices of which a copy is desired
	 * @return a ranged list of the elements contained in this 
	 * 	{@link RangedList} that fit in the provided range. 
	 * @throws IllegalArgumentException if <em>begin</em> is superior to 
	 * <em>end</em>.
	 */
	default public RangedList<T> subList(LongRange range) {
		return subList(range.from, range.to);
	}

	/**
	 * Returns the elements contained in this instance in an array
	 * @return array containing the objects contained in this instance
	 */
	Object[] toArray();

	/**
	 * Returns the elements contained in this instance in an array
	 * @param newRange the range of elements to take 
	 * @return an object array containing the elements of this instance within 
	 * 	the specified range
	 */
	Object[] toArray(LongRange newRange);

	/**
	 * Creates a Chunk containing the elements of this instance contained in the
	 * specified range
	 * @param newRange the range of elements to create a {@link Chunk} with. 
	 * @return a new {@link Chunk} with the specified range containing the
	 * elements of this instance
	 */
	Chunk<T> toChunk(LongRange newRange);

}
