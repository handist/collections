package handist.collections;

import handist.collections.function.LongTBiConsumer;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

abstract public class RangedListGeneral<T> extends RangedList<T> {

    @Override
    protected <U> void forEachImpl(LongRange range, BiConsumer<? super T, Consumer<? super U>> action,
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
    @Override
    public void forEachImpl(LongRange range, Consumer<? super T> action) {
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
    @Override
    public void forEachImpl(LongRange range, LongTBiConsumer<? super T> action) {
        rangeCheck(range);
        Object[] a = getBody();
        int index = (int)(range.from - getBodyOffset());
        for (long i = range.from; i < range.to; i++) {
            action.accept(i, (T)a[index++]);
        }
    }

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

    public abstract Iterator<T> iterator();
    public abstract RangedListIterator<T> listIterator();
    public abstract RangedListIterator<T> listIterator(long from);

    protected abstract Iterator<T> subIterator(LongRange range);
    protected abstract RangedListIterator<T> subListIterator(LongRange range);
    protected abstract RangedListIterator<T> subListIterator(LongRange range, long from);

    /**
     * Iterates on the elements of this instance and the {@code target} and apply the given function to the element.
     * @param range the range on which to apply the method
     * @param target
     * @param func function that receives two object (type T and U) extracted from two ranged list and does not return result.
     * @param <U> the type handled by the {@link RangedList} given as parameter,
     *             second input for the function
     */
    @Override
    protected <U> void mapImpl(LongRange range, RangedList<U> target, BiConsumer<T,U> func) {
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
    @Override
    protected <S> void setupFromImpl(LongRange range, RangedList<S> source, Function<? super S, ? extends T> func) {
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
    @Override
    protected <S,U> void setupFromImpl(LongRange range, RangedList<S> source1, RangedList<U> source2, BiFunction<S,U,T> func) {
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
}
