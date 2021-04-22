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

import static apgas.Constructs.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import apgas.GlobalRuntime;
import handist.collections.dist.DistBag;
import handist.collections.dist.Reducer;

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
public class Bag<T> implements ParallelReceiver<T>, Serializable, KryoSerializable {

    /**
     * Iterator class for {@link Bag}
     */
    private class It implements Iterator<T> {
        /** Iterator on the contents of a List<T> */
        Iterator<T> cIter;
        /** Iterator on {@link Bag#bags}, iterates on lists of T */
        Iterator<List<T>> oIter;

        /**
         * Constructor. Initializes the two iterators used to iterate on the lists
         * contained in {@link Bag#bags} and the iterator on these lists.
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
            while (true) {
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
     * Container of type T. The instances are split in multiple lists, one for each
     * of the calls made to {@link #getReceiver()}.
     */
    ConcurrentLinkedDeque<List<T>> bags;

    /**
     * Default constructor.
     */
    public Bag() {
        bags = new ConcurrentLinkedDeque<>();
    }

    /**
     * Constructor to create a Bag with the same contents as another {@link Bag} or
     * {@link DistBag}.
     *
     * @param bag the bag to copy
     */
    public Bag(Bag<T> bag) {
        bags = bag.bags;
    }

    /**
     * Adds a instances contained by the provided {@link Bag} to this instance.
     *
     * @param bag Bag of T
     */
    public void addBag(Bag<T> bag) {
        bags.addAll(bag.bags);
    }

    /**
     * Adds a list of T instances to this instance.
     *
     * @param l list of T
     */
    public void addBag(List<T> l) {
        bags.add(l);
    }

    /**
     * Removes all contents from this bag.
     */
    @Override
    public void clear() {
        bags.clear();
    }

    /**
     * Copies this {@link Bag}. The individual T elements contained in this instance
     * are not cloned, both this instance and the returned instance will share the
     * same objects.
     */
    @Override
    public Bag<T> clone() {
        final Bag<T> result = new Bag<>();
        for (final Collection<T> bag : bags) {
            final ArrayList<T> nbag = new ArrayList<>(bag);
            result.addBag(nbag);
        }
        return result;
    }

    @Override
    public boolean contains(Object v) {
        for (final Collection<T> bag : bags) {
            if (bag.contains(v)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the bag into a list and clears the bag.
     *
     * @return the contents of this instance as a list
     */
    public List<T> convertToList() {
        // TODO: prepare a smarter implementation
        final ArrayList<T> result = new ArrayList<>(this.size());
        for (final List<T> c : bags) {
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
     * Launches a parallel forEach on the elements of this collection. The elements
     * contained in the individual lists (created either through
     * {@link #addBag(List)} or {@link #getReceiver()}) are submitted to the
     * provided {@link ExecutorService}. This method than waits for the completion
     * of the tasks to return.
     *
     * @param pool   the executor service in charge of processing this instance
     * @param action the action to perform on individual elements
     */
    public void forEach(ExecutorService pool, final Consumer<? super T> action) {
        final List<Future<?>> futures = forEachConst(pool, action);
        for (final Future<?> f : futures) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new ParallelExecutionException("[Bag] exception raised by worker threads.", e);
            }
        }
    }

    private List<Future<?>> forEachConst(ExecutorService pool, final Consumer<? super T> action) {
        final ArrayList<Future<?>> futures = new ArrayList<>();
        for (final Collection<T> bag : bags) {
            futures.add(pool.submit(() -> {
                bag.forEach(action);
            }));
        }
        return futures;
    }

    private void forEachParallelBody(final Consumer<List<T>> run) {
        final Bag<T> separated = this.separate(Runtime.getRuntime().availableProcessors() * 2);
        for (final List<T> sub : separated.bags) {
            async(() -> {
                run.accept(sub);
            });
        }
    }

    /**
     * Adds a new list to this instance and returns a {@link Consumer} which will
     * place the T instances it receives into this dedicated list.
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
        for (final Collection<T> bag : bags) {
            if (!bag.isEmpty()) {
                return false;
            }
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

    /**
     * Returns the number of parallel lists this bag contains. Note that the
     * returned value may be actually different from the actual value if the
     * {@link #getReceiver()} method is called concurrently.
     *
     * @return number of lists in this bag
     */
    public int listCount() {
        return bags.size();
    }

    /**
     * Launches a parallel forEach on the elements of this collection. The elements
     * contained in the individual lists (created either through
     * {@link #addBag(List)} or {@link #getReceiver()}) are submitted to the
     * provided {@link ExecutorService}. This method than waits for the completion
     * of the tasks to return.
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

    /**
     * Performs the specified reduction on the elements contained in this bag in
     * parallel. This {@link Bag} will be emptied as a result
     *
     * @param <R>         type of the reducer
     * @param reducer     instance into which the result will be placed
     * @param parallelism the maximum number of concurrent threads allocated to this
     *                    reduction operation (must be greater or equal to 1)
     * @return the instance given as parameter after the reduction has terminated
     */
    public <R extends Reducer<R, T>> R parallelReduce(int parallelism, R reducer) {
        final ExecutorService pool = GlobalRuntime.getRuntime().getExecutorService();
        final ArrayList<Future<R>> reducerInstances = new ArrayList<>(parallelism);

        for (int i = 0; i < parallelism; i++) {
            // The individual job consists in reducing every element in every list present
            // in member #bags until we run out of lists
            final R r = reducer.newReducer();
            final Future<R> future = pool.submit(() -> {
                List<T> list = bags.poll();
                while (list != null) {
                    for (final T t : list) {
                        r.reduce(t);
                    }
                    list = bags.poll();
                }
            }, r);
            reducerInstances.add(future);
        }

        // Here we wait for each future to terminate and we merge then into the instance
        // given as parameter
        for (final Future<R> future : reducerInstances) {
            try {
                reducer.merge(future.get());
            } catch (final InterruptedException e) {
                e.printStackTrace();
            } catch (final ExecutionException e) {
                e.printStackTrace();
            }
        }

        // All the individual reducers have been merged into the given instance, we
        // return this result
        return reducer;
    }

    /**
     * Performs the specified reduction on the elements contained in this bag in
     * parallel. This {@link Bag} will be emptied as a result
     *
     * @param <R>     type of the reducer
     * @param reducer instance into which the result will be placed
     * @return the instance given as parameter after the reduction has terminated
     */
    public <R extends Reducer<R, T>> R parallelReduce(R reducer) {
        return parallelReduce(Runtime.getRuntime().availableProcessors(), reducer);
    }

    /**
     * Performs the reduction operation specified as parameter in parallel with the
     * specified level of parallelism. This {@link Bag} is emptied as a result.
     *
     * @param <R>         the type of the reducer
     * @param parallelism the maximum number of concurrent threads allocated to this
     *                    reduction operation (must be greater or equal to 1)
     * @param reducer     the instance into which the reduction is going to be
     *                    performed
     * @return the instance specified as parameter after the reduction has completed
     *         and has been fully reduced into that instance
     */
    public <R extends Reducer<R, List<T>>> R parallelReduceList(final int parallelism, final R reducer) {
        final ExecutorService pool = GlobalRuntime.getRuntime().getExecutorService();
        final ArrayList<Future<R>> reducerInstances = new ArrayList<>(parallelism);

        for (int i = 0; i < parallelism; i++) {
            // The individual job consists in reducing every list present in member #bags
            // until we run out of lists
            final R r = reducer.newReducer();
            final Future<R> future = pool.submit(() -> {
                List<T> list = bags.poll();
                while (list != null) {
                    r.reduce(list);
                    list = bags.poll();
                }
            }, r);
            reducerInstances.add(future);
        }

        // Here we wait for each future to terminate and we merge then into the instance
        // given as parameter
        for (final Future<R> future : reducerInstances) {
            try {
                reducer.merge(future.get());
            } catch (final InterruptedException e) {
                e.printStackTrace();
            } catch (final ExecutionException e) {
                e.printStackTrace();
            }
        }

        // All the individual reducers have been merged into the given instance, we
        // return this result
        return reducer;
    }

    /**
     * Performs the reduction operation in parallel using the host's level of
     * parallelism. This {@link Bag} instance will be emptied as a result of this
     * method being called.
     *
     * @param <R>     the type of the reducer used
     * @param reducer instance in which the reduction is going to be performed
     * @return the instance specified as parameter after the reduction has completed
     */
    public <R extends Reducer<R, List<T>>> R parallelReduceList(R reducer) {
        return parallelReduceList(Runtime.getRuntime().availableProcessors(), reducer);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(Kryo kryo, Input input) {
        final int size = input.readInt();
        bags = new ConcurrentLinkedDeque<>();
        final ArrayList<T> bag1 = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            bag1.add((T) kryo.readClassAndObject(input));
        }
        bags.add(bag1);
    }

    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        final int size = in.readInt();
        bags = new ConcurrentLinkedDeque<>();
        final ArrayList<T> bag1 = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            bag1.add((T) in.readObject());
        }
        bags.add(bag1);
    }

    /**
     * Sequentially reduces all the elements contained in this bag using the reducer
     * provided as parameter
     *
     * @param <R>     type of the reducer
     * @param reducer reducer to be used to reduce this parameter
     * @return the reducer provided as parameter after the reduction has completed
     */
    public <R extends Reducer<R, T>> R reduce(R reducer) {
        forEach(t -> reducer.reduce(t));
        return reducer;
    }

    /**
     * Sequentially reduces all the lists of Ts contained in this bag into the
     * provided reducer and returns that reducer.
     *
     * @param <R>     the type of the reducer
     * @param reducer the reducer into which this bag needs to be reduced
     * @return the reducer given as parameter after it has been applied to every
     *         list in this {@link Bag}
     */
    public <R extends Reducer<R, List<T>>> R reduceList(R reducer) {
        for (final List<T> list : bags) {
            reducer.reduce(list);
        }
        return reducer;
    }

    /**
     * Removes one element contained in this instance and returns it. If there are
     * no elements in this instance, returns {@code null}.
     *
     * @return the element removed from this collection, or {@code null} if this
     *         instance does not contain anything
     */
    public synchronized T remove() {
        while (true) {
            if (bags.isEmpty()) {
                return null;
            }
            final List<T> bag = bags.getLast();
            if (bag.isEmpty()) {
                bags.removeLast();
            } else {
                return bag.remove(bag.size() - 1);
            }
        }
    }

    /**
     * Removes and returns {@code n} elements from this instance into a list. If
     * there are less than {@code n} elements contained in this instance, removes
     * and returns all the elements. If this instance is empty, returns an empty
     * list.
     *
     * @param n the number of elements to remove from this instance
     * @return a list containing at most {@code n} elements
     */
    public synchronized List<T> remove(int n) {
        final ArrayList<T> result = new ArrayList<>(n);
        while (n > 0) {
            if (bags.isEmpty()) {
                return result;
            }
            final List<T> bag = bags.getLast();
            if (bag.isEmpty()) {
                bags.removeLast();
                continue;
            }
            result.add(bag.remove(bag.size() - 1));
            n--;
        }
        return result;
    }

    /**
     * Separates the contents of the Bag in <em>n</em> parts. This can be used to
     * apply a forEach method in parallel using 'n' threads for instance. The method
     * returns Bag containing <em>n</em> {@link List}.
     *
     * @param n the number of parts in which to split the Bag
     * @return {@link Bag} containing the same number of elements {@link List}s
     */
    public Bag<T> separate(int n) {
        final int totalNum = this.size();
        final int rem = totalNum % n;
        final int quo = totalNum / n;
        final Bag<T> result = new Bag<>();
        final Iterator<T> it = this.iterator();

        for (int i = 0; i < n; i++) {
            final List<T> r = new ArrayList<>();
            result.addBag(r);
            int rest = quo + ((i < rem) ? 1 : 0);
            while (rest > 0) {
                r.add(it.next());
                rest--;
            }
        }
        return result;
    }

    @Override
    public int size() {
        int size = 0;
        for (final Collection<T> bag : bags) {
            size += bag.size();
        }
        return size;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("[Bag]");
        for (final Collection<T> bag : bags) {
            sb.append(bag.toString() + ":");
        }
        sb.append("end of Bag");
        return sb.toString();
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(size());
        for (final Collection<T> bag : bags) {
            for (final T item : bag) {
                kryo.writeClassAndObject(output, item);
            }
        }
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        // System.out.println("writeChunk:"+this);
        out.writeInt(size());
        for (final Collection<T> bag : bags) {
            for (final T item : bag) {
                out.writeObject(item);
            }
        }
    }
}
