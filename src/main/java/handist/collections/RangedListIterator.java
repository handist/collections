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

import java.util.Iterator;
import java.util.ListIterator;

/**
 * Iterator interface for collections based on {@code long} indices. It allows
 * traveling the elements of the underlying collection in both directions.
 * <p>
 * The design of this iterator is analogous to that of {@link ListIterator}. 
 * However, the type of the indices manipulated were changed to {@code long} and
 * methods that cannot be called on {@link RangedList} implementation were not 
 * kept. 
 * @param <T> type of objects on which this iterator operates
 */
public interface RangedListIterator<T> extends Iterator<T> {

	/**
	 * Returns true if this list iterator has more elements when traversing the 
	 * list in the reverse direction. (In other words, returns true if 
	 * {@link #previous()} would return an element rather than throwing an 
	 * exception.)
	 * @return {@code true} if the iterator has more elements when traversing
	 * the list in the reverse direction
	 */
	public boolean hasPrevious();

	/**
	 * Returns the {@code long} index in {@link RangedList} of the element that
	 * would be returned by calling method {@link #next()}
	 * @return the index of the element that calling {@link #next()} would 
	 * 	return
	 */
	public long nextIndex();

	/**
	 * Returns the previous element in the {@link RangedList} and moves the 
	 * cursor position backwards. This method may be called repeatedly to 
	 * iterate through the list backwards, or intermixed with calls to 
	 * {@link #next()} to go back and forth. Note that alternating calls to 
	 * {@link #next()} and {@link #previous()} will return the same element
	 * repeatedly. 
	 * @return the previous element in the RangedList
	 */
	public T previous();

	/**
	 * Returns the index of the element that would be returned by a subsequent
	 * call to method {@link #previous()}. (If this iterator is at the beginning
	 * of the {@link RangedList}, it will return the lower bound value 
	 * subtracted by 1).
	 * @return the index in the {@link RangedList} 
	 */
	public long previousIndex();

	/**
	 * Replaces the last element returned by {@link #next()} of 
	 * {@link #previous()} with the specified element. 
	 * @param e the value to set in place of the previously returned object
	 * @throws IllegalStateException if neither {@link #next()} nor 
	 * {@link #previous()} have been previously called
	 */
	public void set(T e);
}
