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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import handist.collections.function.LongTBiConsumer;

public interface RangedList<T> extends Iterable<T> {

    RangedList<T> cloneRange(LongRange newRange);

    abstract boolean contains(Object o);
    default public <U> void forEach(BiConsumer<? super T, Consumer<? super U>> action, Consumer<? super U> receiver) {
        forEach(getRange(), action, receiver);
    }

    default public void forEach(Consumer<? super T> action) {
        forEach(getRange(), action);
    };

    default public <U> void forEach(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
            Consumer<? super U> receiver) {
        for (long i = range.from; i < range.to; i++) {
            action.accept(get(i), receiver);
        }   
    }

    default public void forEach(LongRange range, Consumer<? super T> action) {
        for (long i = range.from; i < range.to; i++) {
            action.accept(get(i));
        }   
    }

    default public void forEach(LongRange range, LongTBiConsumer<? super T> action) {
        for (long i = range.from; i < range.to; i++) {
            action.accept(i, get(i));
        }   
    }
    
    
    default public void forEach(LongTBiConsumer<? super T> action) {
        forEach(getRange(), action);
    }

    T get(long index);

    LongRange getRange();

    default public boolean isEmpty() {
        return getRange().size() == 0;
    }

    public Iterator<T> iteratorFrom(long i);
    long longSize();
    default public <U> RangedList<U> map(Function<? super T, ? extends U> func) {
        Chunk<U> result = new Chunk<>(this.getRange());
        result.setupFrom(this, func);
        return result;
    }

    // TODO
    // forEach(pool, nthread) and map(pool, nthreads) will be implemented using
    // those of ChunkedList

    default public <U> RangedList<U> map(LongRange range, Function<? super T, ? extends U> func) {
        return this.subList(range.from, range.to).map(func);
    }

    default public void rangeCheck(long target) {
        if(!this.getRange().contains(target)) {
            throw new IndexOutOfBoundsException(
                "[RangedList] range missmatch:" + this.getRange() + " must includes " + target);
        }
    }
    
    default public void rangeCheck(LongRange target) {
        if(!this.getRange().contains(target)) {
            throw new IndexOutOfBoundsException(
                "[RangedList] range missmatch:" + this.getRange() + " must includes " + target);
        }
    }
    
    public T set(long index, T value);  
    
    abstract public <S> void setupFrom(RangedList<S> from, Function<? super S, ? extends T> func);
    default public List<RangedList<T>> splitRange(long splitPoint) {
        LongRange range = getRange();
        RangedList<T> rangedList1 = new RangedListView<T>(this, new LongRange(range.from, splitPoint));
        RangedList<T> rangedList2 = new RangedListView<T>(this, new LongRange(splitPoint, range.to));
        return Arrays.asList(rangedList1, rangedList2);
    }
    default public List<RangedList<T>> splitRange(long splitPoint1, long splitPoint2) {
        LongRange range = getRange();
        RangedList<T> rangedList1 = new RangedListView<T>(this, new LongRange(range.from, splitPoint1));
        RangedList<T> rangedList2 = new RangedListView<T>(this, new LongRange(splitPoint1, splitPoint2));
        RangedList<T> rangedList3 = new RangedListView<T>(this, new LongRange(splitPoint2, range.to));
        return Arrays.asList(rangedList1, rangedList2, rangedList3);
    }
    /**
     * Provides a RangedList of the elements contained in this object from index
     * <em>begin</em> to index <em>end</em>. 
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

    default public RangedList<T> subList(LongRange range) {
        return subList(range.from, range.to);
    };
    public Object[] toArray();
    public Object[] toArray(LongRange newRange);

	public Chunk<T> toChunk(LongRange newRange);
	
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

}
