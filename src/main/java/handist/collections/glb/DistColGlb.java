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
package handist.collections.glb;

import java.io.Serializable;
import java.util.function.Consumer;

import apgas.util.GlobalID;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.DistBag;
import handist.collections.dist.DistCol;
import handist.collections.dist.Reducer;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.SerializableFunction;
import handist.collections.function.SerializableLongTBiConsumer;
import handist.collections.function.SerializableSupplier;
import handist.collections.glb.GlbComputer.WorkerInfo;

/**
 * This class proposes various operations that operate on all the elements of a
 * {@link DistCol} as part of a GLB program. Any call to methods of this class
 * should be made from within a
 * {@link GlobalLoadBalancer#underGLB(apgas.SerializableJob)} method.
 *
 * @author Patrick Finnerty
 * @param <T> type of the elements contained in the underlying distributed
 *            collection
 */
public class DistColGlb<T> extends AbstractGlbHandle implements Serializable {

    /**
     * Runtime exception used when a {@link Throwable} is thrown from a closure
     * given as parameter of a Glb operation.
     *
     * @author Patrick Finnerty
     *
     */
    public static class DistColGlbError extends RuntimeException implements Serializable {

        /** Serial Version UID */
        private static final long serialVersionUID = -6284960496356484016L;

        /** Index in the {@link DistCol} on which a problem was encountered */
        public final long index;

        /**
         * Range on which the assignment was operating at the time the exception was
         * encountered
         */
        public final LongRange assignmentRange;

        /**
         * Constructor
         * <p>
         * This constructor is made private as instances of this class do not need to be
         * created outside of {@link DistColGlb}.
         *
         * @param lr range on which the assignment was operating
         * @param l  index at which the throwable was thrown
         * @param t  the {@link Throwable} thrown by the user-supplied closure
         */
        private DistColGlbError(LongRange lr, long l, Throwable t) {
            super(t.getMessage() + " at index " + l + " in assignment on range " + lr, t);
            assignmentRange = new LongRange(lr.from, lr.to);
            index = l;
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 612021438330155918L;

    /** Underlying collection on which the operations of this class operate */
    DistCol<T> col;

    /**
     * Constructor
     *
     * @param c collection on which this handle will operate
     */
    public DistColGlb(DistCol<T> c) {
        col = c;
    }

    /**
     * Applies the specified action to all the elements contained in the
     * {@link DistCol} and returns the underlying collection
     *
     * @param action action to perform on each element
     * @return future representing this "forEach" operation which will return the
     *         underlying {@link DistCol} collection upon termination
     */
    public DistFuture<DistCol<T>> forEach(SerializableConsumer<T> action) {
        final GlobalLoadBalancer glb = getGlb();

        // Initialize the future returned to the programmer in the underGLB method
        // In this operation, the collection involved is the handle itself
        final DistFuture<DistCol<T>> future = new DistFuture<>(col);

        final SerializableSupplier<GlbTask> initGlbTask = () -> {
            return new DistColGlbTask(col);
        };

        // We transform the action to accept a LongRange as parameter, retrieve
        // the T at the each index, and apply the lambda given as parameter to these Ts
        // The second argument (WorkerService) provided by the GLB runtime is unused for
        // this operation
        final SerializableBiConsumer<LongRange, WorkerService> realAction = (lr, ws) -> {
            final RangedList<T> chunk = col.getChunk(lr);
            for (long l = lr.from; l < lr.to; l++) {
                try {
                    action.accept(chunk.get(l));
                } catch (final Throwable t) {
                    ws.throwableInOperation(new DistColGlbError(lr, l, t));
                }
            }
        };

        // Create the operation with all the types/arguments
        final GlbOperation<DistCol<T>, T, LongRange, LongRange, DistCol<T>> operation = new GlbOperation<>(col,
                realAction, future, initGlbTask, null);
        // Submit the operation to the GLB
        glb.submit(operation);

        // return the future to the programmer
        return future;
    }

    /**
     * Applies the specified action to all the elements contained in the
     * {@link DistCol} and returns the underlying collection
     *
     * @param action action to perform on each element, taking the index and the
     *               object as parameter
     * @return future representing this "forEach" operation which will return the
     *         underlying {@link DistCol} collection upon termination
     */
    public DistFuture<DistCol<T>> forEach(SerializableLongTBiConsumer<T> action) {
        final GlobalLoadBalancer glb = getGlb();

        // Initialize the future returned to the programmer in the underGLB method
        // In this operation, the collection involved is the handle itself
        final DistFuture<DistCol<T>> future = new DistFuture<>(col);

        final SerializableSupplier<GlbTask> initGlbTask = () -> {
            return new DistColGlbTask(col);
        };

        // We transform the action to accept a LongRange as parameter, retrieve
        // the T at the each index, and apply the lambda given as parameter to these Ts
        // The second argument (WorkerService) provided by the GLB runtime is unused for
        // this operation
        final SerializableBiConsumer<LongRange, WorkerService> realAction = (lr, ws) -> {
            final RangedList<T> chunk = col.getChunk(lr);
            for (long l = lr.from; l < lr.to; l++) {
                try {
                    action.accept(l, chunk.get(l));
                } catch (final Throwable t) {
                    ws.throwableInOperation(new DistColGlbError(lr, l, t));
                }
            }
        };

        // Create the operation with all the types/arguments
        final GlbOperation<DistCol<T>, T, LongRange, LongRange, DistCol<T>> operation = new GlbOperation<>(col,
                realAction, future, initGlbTask, null);
        // Submit the operation to the GLB
        glb.submit(operation);

        // return the future to the programmer
        return future;
    }

    /**
     * GLB operation which creates a new {@link DistCol} using the mapping operation
     * provided as parameter. The resulting {@link DistCol} will contain the same
     * indices as this collection. The value stored at each index of the resulting
     * collection will be the result of the provided mapping operation for this
     * collection at the same index. As part of the GLB consists in moving entries
     * from place to place, it is possible for the distribution of the resulting
     * collection and this collection to differ.
     *
     * @param <U> type of the result of the map function provided as parameter
     * @param map function which takes an object T as input and returns a instance
     *            of type U
     * @return a {@link DistFuture}
     */
    public <U> DistFuture<DistCol<U>> map(SerializableFunction<T, U> map) {
        final GlobalLoadBalancer glb = getGlb();

        // Create new collection to contain the result
        final DistCol<U> resultCollection = new DistCol<>(col.placeGroup());

        // Adapt the provided map to represent what the glb workers will actually
        // perform.
        // The second argument (WorkerService) provided by the GLB runtime is unused for
        // this operation
        final SerializableBiConsumer<LongRange, WorkerService> realAction = (lr, ws) -> {
            // First, initialize a Chunk to place the mappings
            final Chunk<U> c = new Chunk<>(lr);

            resultCollection.add(c);

            // Iterate on the elements
            final RangedList<T> chunk = col.getChunk(lr);
            for (long l = lr.from; l < lr.to; l++) {
                try {
                    final T t = chunk.get(l);
                    final U u = map.apply(t);
                    c.set(l, u);
                } catch (final Throwable t) {
                    ws.throwableInOperation(new DistColGlbError(lr, l, t));
                }
            }
        };

        // Initialize the future returned to the programmer in the underGLB method
        // In this operation, the collection involved is the handle itself
        final DistFuture<DistCol<U>> future = new DistFuture<>(resultCollection);

        final SerializableSupplier<GlbTask> initGlbTask = () -> {
            return new DistColGlbTask(col);
        };

        // Create the operation with all the types/arguments
        final GlbOperation<DistCol<T>, T, LongRange, LongRange, DistCol<U>> operation = new GlbOperation<>(col,
                realAction, future, initGlbTask, null);

        // Submit the operation to the GLB
        glb.submit(operation);

        // return the future to the programmer
        return future;
    }

    @SuppressWarnings("unchecked")
    public <R extends Reducer<R, T>> DistFuture<R> reduce(final R reducer) {
        final GlobalLoadBalancer glb = getGlb();
        final GlobalID gid = new GlobalID();
        final R globalReducer = reducer;

        final SerializableConsumer<WorkerService> workerInit = (w) -> w.attachOperationObject(gid,
                globalReducer.newReducer());

        final SerializableBiConsumer<LongRange, WorkerService> realAction = (lr, ws) -> {
            final R workerLocalReducer = (R) ws.retrieveOperationObject(gid);

            final RangedList<T> chunk = col.getChunk(lr);
            for (long l = lr.from; l < lr.to; l++) {
                try {
                    workerLocalReducer.reduce(chunk.get(l));
                } catch (final Throwable t) {
                    ws.throwableInOperation(new DistColGlbError(lr, l, t));
                }
            }
        };

        final DistFuture<R> future = new DistFuture<>(globalReducer);

        final SerializableSupplier<GlbTask> initGlbTask = () -> {
            return new DistColGlbTask(col);
        };

        final GlbOperation<DistCol<T>, T, LongRange, LongRange, R> operation = new GlbOperation<>(col, realAction,
                future, initGlbTask, workerInit);

        glb.submit(operation);

        // This operation needs a specific hook after all the entries have been
        // traversed. We need to reduce all the R instances that were created back into
        // a single instance on each host, and perform the global reduction such that
        // the given reducer contains the global result of the operation.
        operation.addHook(() -> {
            col.placeGroup().broadcastFlat(() -> {
                final R localReducer = reducer; // (R) gid.getHere();
                for (final WorkerInfo wi : GlbComputer.getComputer().workers) {
                    localReducer.merge((R) wi.workerBoundObjects.remove(gid));
                }
                localReducer.teamReduction(col.placeGroup());
            });
        });

        return future;
    }

    /**
     * Applies the given function to every element contained in this distributed
     * collection and places the results in a new {@link DistBag} collection.
     *
     * @param <U>      type of the objects produced by the function given as
     *                 parameter
     * @param function function taking type T as input and returning U
     * @return a {@link DistFuture} producing a DistBag as a result
     */
    public <U> DistFuture<DistBag<U>> toBag(SerializableFunction<T, U> function) {
        final GlobalLoadBalancer glb = getGlb();

        // Create new collection to contain the result
        final DistBag<U> resultCollection = new DistBag<>(col.placeGroup());

        // Initialization for workers to be made before the computation starts.
        // This will bind a handle to place the U elements into the DistBag to each
        // worker in the system.
        final SerializableConsumer<WorkerService> workerInit = (w) -> w.attachOperationObject(resultCollection,
                resultCollection.getReceiver());

        // Adapt the provided function to represent what the glb workers will actually
        // perform
        final SerializableBiConsumer<LongRange, WorkerService> realAction = (lr, ws) -> {
            // First, retrieve the consumer of U which is bound to the worker
            // The object used as key to retrieve the object bound to workers is the result
            // collection
            @SuppressWarnings("unchecked")
            final Consumer<U> destination = (Consumer<U>) ws.retrieveOperationObject(resultCollection);

            // Iterate on the elements
            final RangedList<T> chunk = col.getChunk(lr);
            for (long l = lr.from; l < lr.to; l++) {
                try {
                    final T t = chunk.get(l);
                    final U u = function.apply(t);
                    destination.accept(u);
                } catch (final Throwable t) {
                    ws.throwableInOperation(new DistColGlbError(lr, l, t));
                }
            }
        };

        // Initialize the future returned to the programmer in the underGLB method
        // The result of this operation is the DistBag "resultCollection"
        final DistFuture<DistBag<U>> future = new DistFuture<>(resultCollection);

        // Initializer for GlbTask of this DistCol in case it is not yet initialized
        final SerializableSupplier<GlbTask> initGlbTask = () -> {
            return new DistColGlbTask(col);
        };

        // Create the operation with all the types/arguments
        final GlbOperation<DistCol<T>, T, LongRange, LongRange, DistBag<U>> operation = new GlbOperation<>(col,
                realAction, future, initGlbTask, workerInit);

        // Submit the operation to the GLB
        glb.submit(operation);

        // return the future to the programmer
        return future;
    }
}
