/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.*;

import handist.collections.function.LongTBiConsumer;

/**
 * Abstract class describing a list defined on long indices. Entries can be
 * defined on any index contained within the {@link LongRange} used to
 * initialize the collection.
 *
 * @param <T> type handled by the collection
 */
public abstract class RangedList<T> implements Iterable<T> {

    public static boolean equals(RangedList<?> rlist1, Object o) {
        if (o == null) {
            return (rlist1 == null);
        }
        if (!(o instanceof RangedList)) {
            return false;
        }
        final RangedList<?> rlist2 = (RangedList<?>) o;
        // TODO this version is too slow,
        // setupFrom will be the good candidate for fast simul scanner.
        if (!rlist1.getRange().equals(rlist2.getRange())) {
            return false;
        }
        for (final long index : rlist1.getRange()) {
            if (!rlist1.get(index).equals(rlist2.get(index))) {
                return false;
            }
        }
        return true;
    }

    public static int hashCode(RangedList<?> rlist) {
        int hashCode = 1;
        // code from JavaAPI doc of List
        for (final Object o : rlist) {
            hashCode = 31 * hashCode + (o == null ? 0 : o.hashCode());
        }
        return hashCode;
    }

    /**
     * Returns a copy of this instance, restricted to the contents that are included
     * in the specified range.
     *
     * @param range portion of the {@link RangedList} to copy
     * @return a new RangedList which contains the entries of this instance on
     *         provided range
     */
    public abstract RangedList<T> cloneRange(LongRange range);

    /**
     * Indicates if this list contains the provided object. More formally if the
     * list contains at least one object {@code a} such that
     * <code>(a == null) ? o == null : a.equals(o);</code> is true.
     *
     * @param o the object whose presence is to be checked
     * @return {@code true} if the collection contains {@code o}, {@code false}
     *         otherwise
     */
    public abstract boolean contains(Object o);

    /**
     * Checks if all the elements provided in the collection are present in this
     * instance.
     *
     * @param c collection of all the elements whose presence in the RangedList is
     *          to be checked
     * @return {@code true} if all the elements in the provided collection can be
     *         found in this instance, {@code false} otherwise
     */
    public boolean containsAll(Collection<? extends T> c) {
        for (final T t : c) {
            if (!contains(t)) {
                return false;
            }
        }
        return true;
    }


    /**
     * Performs the provided action on each element contained by this instance, and
     * potentially collect/extract some information into the provided receiver.
     * <p>
     * The BiConsumer is applied on each element contained in the collection (first
     * parameter of the BiConsumer) with the receiver provided as second parameter
     * of this method as the second parameter of the BiConsumer. This allows you to
     * make modifications to individual elements and potentially extract some
     * information (of type U) and store it in the receiver provided as second
     * parameter.
     * <p>
     * If you do not need to extract any information from the elements contained in
     * this instance, you should use {@link #forEach(Consumer)} instead.
     *
     * @param <U>      type of the collected instances
     * @param action   action to perform on each element, potentially
     * @param receiver collector of information extracted
     */
    public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, Consumer<? super U> receiver) {
        forEach(getRange(), action, receiver);
    }

    /**
     * Performs the provided action on every element in the collection
     */
    @Override
    public void forEach(Consumer<? super T> action) {
        forEach(getRange(), action);
    }

    /**
     * Performs the provided action on elements contained by this instance, and
     * potentially collect/extract some information into the provided receiver. This
     * method has the same effect as {@link #forEach(BiConsumer, Consumer)} but its
     * application is restricted to the range specified as first parameter.
     * <p>
     * The BiConsumer is applied on each element contained in the collection (first
     * parameter of the BiConsumer) with the receiver provided as second parameter
     * of this method as the second parameter of the BiConsumer. This allows you to
     * make modifications to individual elements and potentially extract some
     * information (of type U) and store it in the receiver provided as second
     * parameter.
     * <p>
     * If you do not need to extract any information from the elements contained in
     * this instance, you should use {@link #forEach(LongRange, Consumer)} instead.
     *
     * @param <U>      type of the collected instances
     * @param range    range on which the action is to be applied
     * @param action   action to perform on each element, potentially
     * @param receiver collector of information extracted
     * @see #forEach(LongRange, BiConsumer, Consumer)
     */
    public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
            Consumer<? super U> receiver) {
        rangeCheck(range);
        Object[] a = getBody();
        int index = (int) (range.from - getBodyOffset());
        final int limit = (int) (range.to - getBodyOffset());
        while (index < limit) {
            action.accept((T)a[index++], receiver);
        }
    }
    /**
     * Applies the specified action on the elements of this collection that are
     * present in the specified range. This method is similar to
     * {@link #forEach(Consumer)} but the application of the specified action is
     * restricted to the range specified as first parameter
     *
     * @param range  range of application of the action
     * @param action action to perform on individual elements
     */
    public void forEach(LongRange range, Consumer<? super T> action) {
        rangeCheck(range);
        Object[] a = getBody();
        int index = (int) (range.from - getBodyOffset());
        final int limit = (int) (range.to - getBodyOffset());
        while (index < limit) {
            action.accept((T)a[index++]);
        }
    }

    /**
     * Applies the given action on the index/value pairs present in the specified
     * range.
     * <p>
     * This method is almost identical to {@link #forEach(LongTBiConsumer)} but its
     * application is restricted to the range of indices specified as parameter.
     *
     * @param range  range of indices on which to apply the action
     * @param action action to perform taking a long and a T as parameter
     */
    public void forEach(LongRange range, LongTBiConsumer<? super T> action) {
        rangeCheck(range);
        Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        for (long i = range.from; i < range.to; i++) {
            action.accept(i, (T)a[index++]);
        }
    }

    /**
     * Performs the specified action on every index/value pair contained in this
     * collection
     *
     * @param action action to perform taking a long and a T as parameter
     */
    public void forEach(LongTBiConsumer<? super T> action) {
        forEach(getRange(), action);
    }

    /**
     * Returns the value associated with the provided {@code long} index.
     *
     * @param index index of the value to return.
     * @return the value associated with this index
     */
    public abstract T get(long index);

    /**
     * Obtain the {@link LongRange} on which this instance is defined.
     *
     * @return the {@link LongRange} object representing the
     */
    public abstract LongRange getRange();

    /**
     * return the array that contains the elements.  The implementation of {@code forEach}, {@code map}, {@code setupFrom} methods
     * defined in this class assumes such a container array.
     *
     * If you want to implement a subclass of the RangedList that does not use such container arrays (e.g. infinite random sequence),
     * please give the implementation of {@code forEach}, {@code map}, {@code setupFrom} methods for it.
     *
     * @return the container array
     */
    protected abstract Object[] getBody();

    /**
     * return the offset value that Indicates from which the elements of this {@code RangedList} are located in the base container array.
     * @return
     */
    protected abstract long getBodyOffset();

    /**
     * Indicates if this RangedList is empty, i.e. if it cannot contain any entry
     * because it is defined on an empty {@link LongRange}.
     *
     * @return {@code true} is the instance is defined on an empty
     *         {@link LongRange}, {@code false} otherwise.
     */
    public boolean isEmpty() {
        return getRange().size() == 0;
    }

    public abstract RangedIterator<T> iterator();
    public abstract RangedListIterator<T> listIterator();
    public abstract RangedListIterator<T> listIterator(long from);

    protected abstract RangedIterator<T> subIterator(LongRange range);
    protected abstract RangedListIterator<T> subListIterator(LongRange range);
    protected abstract RangedListIterator<T> subListIterator(LongRange range, long from);


    /**
     * Creates a new collection from the elements contained in this instance by
     * transforming them into a new type
     *
     * @param <U>  type of the collection to create
     * @param func function that returns a type U from the provided T
     * @return a newly created collection which contains the mapping of the elements
     *         contained by this instance to type U
     */
    public <U> RangedList<U> map(Function<? super T, ? extends U> func) {
        final Chunk<U> result = new Chunk<>(this.getRange());
        result.setupFrom(this, func);
        return result;
    }

    /**
     * Creates a new collection from the elements contained in this instance on the
     * specified range by transforming them into a different type
     *
     * @param <U>   type of the collection to create
     * @param range the range on which to apply the method
     * @param func  function that returns a type U from the provided T
     * @return a newly created collection which contains the mapping of the elements
     *         contained by this instance (restricted to the specified range) to
     *         type U
     */
    public <U> RangedList<U> map(LongRange range, Function<? super T, ? extends U> func) {
        return this.subList(range.from, range.to).map(func);
    }

    /**
     * Iterates on the elements of this instance and the {@code target} and apply the given function to the element.
     * @param range the range on which to apply the method
     * @param target
     * @param func function that receives two object (type T and U) extracted from two ranged list and does not return result.
     * @param <U> the type handled by the {@link RangedList} given as parameter,
     *             second input for the function
     */
    /*
    public <U> void map(LongRange range, RangedList<U> target, BiConsumer<T,U> func) {
        // This implementation requires getBody() implementation for the target.
        rangeCheck(range);
        target.rangeCheck(range);
        final Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        int limit = (int)(range.to - getBodyOffset());
        final Object[] targetA = target.getBody();
        int tIndex = (int)(range.from - target.getBodyOffset());
        while(index<limit) {
            func.accept((T)a[index++], (U)targetA[tIndex++]);
        }
    }*/

    public <U> void map(LongRange range, RangedList<U> target, BiConsumer<T,U> func) {
        rangeCheck(range);
        target.rangeCheck(range);
        final Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        int limit = (int)(range.to - getBodyOffset());
        Iterator<U> iter = target.subList(range).iterator();
        while(index<limit) {
            func.accept((T)a[index++], iter.next());
        }
    }





    public <S, U> RangedList<U> map(LongRange range, RangedList<S> target, BiFunction<T, S, U> func) {
        final Chunk<U> result = new Chunk<>(range);
        rangeCheck(range);
        target.rangeCheck(range);
        result.setupFrom(range, this, target, func);
        return result;
    }


    static class Box<U> {
        U val;
        Box(U val) {
            this.val = val;
        }
    }

    public T reduce(BiFunction<T,T,T> reduce) {
        Box<T> box = new Box<T>(null);
        forEach((T t)->{
            if(box.val == null) box.val = t;
            else box.val = reduce.apply(box.val, t);
        });
        return box.val;
    }

    public <U> U reduce(BiFunction<U,T,U> reduce, U zero) {
        Box<U> box = new Box<U>(zero);
        box.val=zero;
        forEach((T t)->{
            box.val = reduce.apply(box.val, t);
        });
        return box.val;
    }
    public <U,S> U reduce(RangedList<S> source2, BiFunction<T, S, U> map, U zero, BiFunction<U,U,U> reduce) {

        Box<U> box = new Box<U>(zero);
        box.val=zero;
        map(getRange(), source2, (T t, S s)-> {
            box.val = reduce.apply(box.val,map.apply(t, s));
        });
        return box.val;
    }


    /**
     * Checks if the provided {@code long index} is included in the range this
     * instance is defined on, i.e. if method {@link #get(long)}, or
     * {@link #set(long,Object)} can be safely called with the provided parameter.
     *
     * @param target the index to check
     * @throws IndexOutOfBoundsException if the provided index is outside the range
     *                                   this instance is defined on
     */
    public void rangeCheck(long target) {
        if (!this.getRange().contains(target)) {
            throw new IndexOutOfBoundsException(
                    "[RangedList] range mismatch: " + this.getRange() + " does not include " + target);
        }
    }

    /**
     * Checks if the provided {@link LongRange} is included in the range of this
     * instance.
     *
     * @param target LongRange whose inclusion in this instance is to be checked
     * @throws ArrayIndexOutOfBoundsException if the provided {@link LongRange} is
     *                                        not included in this instance
     */
    public void rangeCheck(LongRange target) {
        if (!this.getRange().contains(target)) {
            throw new ArrayIndexOutOfBoundsException(
                    "[RangedList] range mismatch:" + this.getRange() + " must include " + target);
        }
    }

    /**
     * Sets the provided value at the specified index
     *
     * @param index index at which the value should be stored
     * @param value value to store at the specified index
     * @return previous value that was stored at this index, {@code null} if there
     *         was no previous value or the previous value stored was {@code null}
     */
    public abstract T set(long index, T value);

    /**
     * Initializes the values in this instance by applying the provided function on
     * the elements contained in {@code source}
     *
     * @param <S>    the type handled by the {@link RangedList} given as parameter,
     *               input for the function
     * @param source {@link RangedList} instance from which entried for this
     *               instance will be extracted
     * @param func   function that takes an object of type S as parameter and
     *               returns a type T
     */
    public <S> void setupFrom(RangedList<S> source, Function<? super S, ? extends T> func) {
        setupFrom(source.getRange(), source, func);
    }
    /**
     * Initializes the values in the {code range} of this instance by applying the provided function on
     * the elements contained in {@code source}
     *
     * @param <S>    the type handled by the {@link RangedList} given as parameter,
     *               input for the function
     * @param range  the range where initialization are applied
     * @param source {@link RangedList} instance from which entries for this
     *               instance will be extracted
     * @param func   function that takes an object of type S as parameter and
     *               returns a type T
     */
    public <S> void setupFrom(LongRange range, RangedList<S> source, Function<? super S, ? extends T> func) {
        rangeCheck(range);
        source.rangeCheck(range);
        final Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        final int limit = (int)(range.to - getBodyOffset());
        Iterator<S> iter = source.subIterator(range);
        while(index < limit) {
            a[index++]= func.apply(iter.next());
        };
    }

    /**
     * Initializes the values in the {code range} of this instance by applying the provided function on
     * the elements contained in {@code source1} and {@code source2}
     *
     * @param <S>   the first type handled by the {@link RangedList} given as parameter,
     *               input for the function
     * @param <U>    the second type handled by the {@link RangedList} given as parameter,
     *               input for the function
     * @param range  the range where initialization are applied
     * @param source1 the first {@link RangedList} instance from which entries for this
     *               instance will be extracted
     * @param source2 the second {@link RangedList} instance from which entries for this
     *               instance will be extracted
     * @param func   function that takes two objects of type S and U as parameter and
     *               returns a type T
     */

    public <S,U> void setupFrom(LongRange range, RangedList<S> source1, RangedList<U> source2, BiFunction<S,U,T> func) {
        rangeCheck(range);
        source1.rangeCheck(range);
        source2.rangeCheck(range);
        final Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        int limit = (int)(range.to - getBodyOffset());
        final Iterator<S> iter1 = source1.subIterator(range);
        final Iterator<U> iter2 = source2.subIterator(range);
        while(index < limit) {
            a[index++] = func.apply(iter1.next(), iter2.next());
        }
    }
    /* The following is a implementation that assume getBody() implementation of source lists.
    public <S,U> void setupFrom(LongRange range, RangedList<S> source1, RangedList<U> source2, BiFunction<S,U,T> func) {
        rangeCheck(range);
        source1.rangeCheck(range);
        source2.rangeCheck(range);
        final Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        int limit = (int)(range.to - getBodyOffset());
        final Object[] s1A = source1.getBody();
        int s1index = (int)(range.from - source1.getBodyOffset());
        final Object[] s2A = source2.getBody();
        int s2index = (int)(range.from - source2.getBodyOffset());
        while(index < limit) {
            a[index++] = func.apply((S) s1A[s1index++], (U) s2A[s2index++]);
        }
    }*/

    /**
     * Returns the number of entries in this collection as a {@code long}
     *
     * @return size of the collection
     */
    public long size() {
        final LongRange r = getRange();
        return r.to - r.from;
    }

    /**
     * Separates this instance into multiple {@link RangedList}s using the points
     * given as parameter.
     * <p>
     * For instance, if this instance is defined on a range [a,b) and points l, m,
     * and n are given as parameter, this method will return 4 {@link RangedList}
     * defined on [a,l), [l,m), [m,n), and [n,b).
     * <p>
     * The user will be careful to sort the points given as parameter in ascending
     * order. Exceptions during the creation of {@link RangedList} will be thrown
     * otherwise.
     *
     * @param splitPoints the points at which this instance needs to be cut
     * @return this instance entries split into several {@link RangedList}
     */
    public LinkedList<RangedList<T>> splitRange(long... splitPoints) {
        final LinkedList<RangedList<T>> toReturn = new LinkedList<>();
        final LongRange range = getRange();
        long start = range.from;
        for (final long split : splitPoints) {
            toReturn.add(new RangedListView<>(this, new LongRange(start, split)));
            start = split;
        }
        toReturn.add(new RangedListView<>(this, new LongRange(start, range.to)));
        return toReturn;
    }

    /**
     * Provides a RangedList of the elements contained in this instance from index
     * <em>begin</em> to index <em>end</em>.
     * <p>
     * If the provided range exceeds the indices contained in this instance (i.e. if
     * <em>begin</em> is lower than the lowest index contained in this instance, or
     * if <em>end</em> is higher than the highest index contained in this instance)
     * the method will return the elements it contains that fit within the provided
     * range.
     *
     * @param begin starting index of the desired sub-list
     * @param end   last index of the desired sub-list (exlusive)
     * @return a ranged list of the elements contained in this {@link RangedList}
     *         that fit in the provided range.
     * @throws IllegalArgumentException  if <em>begin</em> is superior to
     *                                   <em>end</em>.
     * @throws IndexOutOfBoundsException if the provided range has no intersection
     *                                   with the range of this instance.
     */
    public RangedList<T> subList(long begin, long end) {
        if (begin > end) {
            throw new IllegalArgumentException("Cannot obtain a sublist from " + begin + " to " + end);
        }
        final long from = Math.max(begin, getRange().from);
        final long to = Math.min(end, getRange().to);
        if (from > to) {
            throw new IndexOutOfBoundsException("[RangedList] no intersection with [" + begin + "," + end + ")");
        }
        final LongRange newRange = new LongRange(from, to);
        if (newRange.equals(getRange())) {
            return this;
        }
        return new RangedListView<>(this, newRange);
    }

    /**
     * Provides a RangedList of the elements contained in this instance on the
     * specified {@link LongRange}.
     * <p>
     * If the provided range exceeds the indices contained in this instance (i.e. if
     * lower bound of the {@link LongRange} is lower than the lowest index contained
     * in this instance, or if the upper bound of the provided {@link LongRange} is
     * higher than the highest index contained in this instance) the method will
     * return the elements it contains that fit within the provided range.
     *
     * @param range range of indices of which a copy is desired
     * @return a ranged list of the elements contained in this {@link RangedList}
     *         that fit in the provided range.
     * @throws IllegalArgumentException if <em>begin</em> is superior to
     *                                  <em>end</em>.
     */
    public RangedList<T> subList(LongRange range) {
        return subList(range.from, range.to);
    }

    /**
     * Returns the elements contained in this instance in an array
     *
     * @return array containing the objects contained in this instance
     */
    public abstract Object[] toArray();

    /**
     * Returns the elements contained in this instance in an array
     *
     * @param r the range of elements to take
     * @return an object array containing the elements of this instance within the
     *         specified range
     */
    public abstract Object[] toArray(LongRange r);

    /**
     * Creates a Chunk containing all the elements of this instance
     *
     * @return a new {@link Chunk} with the same range as this instance containing
     *         all the elements of this instance
     */
    public Chunk<T> toChunk() {
        return toChunk(getRange());
    }

    /**
     * Creates a Chunk containing the elements of this instance included in the
     * specified range
     *
     * @param r the range of elements to create a {@link Chunk} with.
     * @return a new {@link Chunk} with the specified range containing the elements
     *         of this instance
     */
    public abstract Chunk<T> toChunk(LongRange r);

    /**
     * Returns the elements contained in this instance in a {@link List}. Note that
     * the indices of the returned list do not reflect the long indices used in this
     * implementation.
     *
     * @return a list containing the elements of this instance within the specified
     *         range
     */
    public List<T> toList() {
        return toList(getRange());
    }

    /**
     * Returns the elements contained in this instance within the specified range in
     * a {@link List}. Note that the indices of the returned list do not reflect the
     * long indices used in this implementation.
     *
     * @param r the range of indices of this instance to include in the returned
     *          list
     * @return a list containing the elements of this instance within the specified
     *         range
     */
    public abstract List<T> toList(LongRange r);
}
