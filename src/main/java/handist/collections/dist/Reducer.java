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
package handist.collections.dist;

import java.io.Serializable;
import java.util.HashMap;

import apgas.util.GlobalID;
import mpi.Datatype;
import mpi.MPI;
import mpi.Op;
import mpi.User_function;

/**
 * Reducer object. This object provides an abstract reduction operation on some
 * data element T. Implementations should provide the {@link #reduce(Object)},
 * {@link #merge(Reducer)} and {@link #newReducer()} methods.
 * <p>
 * As an example implementation, we provide the example below which performs the
 * sum over {@link Integer}s.
 *
 * <pre>
 * class SumReduction extends Reducer&lt;SumReduction, Integer&gt; {
 *     int runningSum = 0;
 *
 *     public void merge(SumReduction r) {
 *         runningSum += r.runningSum;
 *     }
 *
 *     public void reduce(Integer i) {
 *         runningSum += i;
 *     }
 *
 *     public void newReducer() {
 *         return new SumReduction();
 *     }
 * }
 * </pre>
 *
 * @author Patrick Finnerty
 *
 * @param <R> the implementing type itself, this is used to correctly implement
 *            reflective methods (methods that act on the implementing type
 *            itself)
 * @param <T> the type from which data is acquired to compute the reduction
 */
public abstract class Reducer<R extends Reducer<R, T>, T> extends User_function implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 956660189595987110L;

    /**
     * The reduction operations which have been registered for use with the MPI
     * reduce calls. The canonical name of the class implementing the reduction
     * operation is used as key in this map. As calls to methods
     * {@link #globalReduction(TeamedPlaceGroup, GlobalID)} and
     * {@link #teamReduction(TeamedPlaceGroup)} are made, new entries are
     * initialized and added to this map to be reused when a new reduction using the
     * same Reducer implementation is made.
     */
    private static transient HashMap<String, Op> registeredUserOperations = new HashMap<>();

    /**
     * This method is called by the MPI runtime to make the reduction happen between
     * objects. You do not need to override this method, and do so at your own
     * peril.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void Call(Object firstOperandArray, int firstOperandArrayOffset, Object secondOperandAndResultArray,
            int secondOperandArrayOffset, int count, Datatype datatype) {
        // The argument casts are somewhat tedious but Java.lang.ClassCastException
        // are thrown if we try to perform then in one go.
        final Object[] ov = (Object[]) secondOperandAndResultArray;
        final Object[] iv = (Object[]) firstOperandArray;

        for (int i = secondOperandArrayOffset,
                j = firstOperandArrayOffset; i < count + secondOperandArrayOffset; i++, j++) {
            final R result = (R) ov[i];
            final R operand = (R) iv[j];
            result.merge(operand);
        }
    }

    /**
     * Performs the global reduction of this instance. This method needs to be
     * called when the reduction operation took place over data distributed across
     * multiple hosts. In such a case, the reducer will have one instance per host
     * in which the local result if contained. This method is used to merge the
     * results of all the local reducers and make them reflect the global result of
     * the reduction.
     * <p>
     * An implementation does not need to override this method. However, users
     * should make sure that method {@link #merge(Reducer)} was appropriately
     * implemented. Otherwise, problems may arise.
     *
     * @param placeGroup the group of places on which "local" reductions have been
     *                   performed
     * @param gid        global id under which each local instance is registered
     * @throws IllegalStateException if this instance is not allowed to perform
     *                               global reductions
     */
    /*
     * This method needs improvements. More likely class Reducer needs some
     * modifications to handle global reduction operations that are not performed
     * under the GLB. Currently, the registering of a local Reducer instance needs
     * to be performed manually by the programmer, which does not satisfactorily
     * provides the global reduction features.
     */
    @SuppressWarnings("deprecation")
    public void globalReduction(TeamedPlaceGroup placeGroup, GlobalID gid) {
        if (placeGroup == null || gid == null) {
            throw new IllegalArgumentException("Method Reducer#globalReduction does not tolerate null parameters");
        }

        final int reductionRank = placeGroup.myrank;

        placeGroup.broadcastFlat(() -> {
            // Retrieve the local object through the global id
            @SuppressWarnings("unchecked")
            final Reducer<R, T> local = (Reducer<R, T>) gid.getHere();

            // Prepare the reduction through the MPI reduceAll call
            // 1. We need to make sure that the reducer instance was registered in the
            // operations
            Op mpiOperation;
            final String s = local.getClass().getCanonicalName();
            synchronized (registeredUserOperations) {
                if (!registeredUserOperations.containsKey(s)) {
                    mpiOperation = new Op(local.newReducer(), true);
                    registeredUserOperations.put(s, mpiOperation);
                } else {
                    mpiOperation = registeredUserOperations.get(s);
                }
            }
            // 2. Prepare the local instances into an Object Array
            final Object[] buffer = new Object[1];
            buffer[0] = local;

            // 3. Make the MPI AllReduce call
            placeGroup.comm.Reduce(buffer, 0, buffer, 0, 1, MPI.OBJECT, mpiOperation, reductionRank);
        });
    }

    /**
     * Merges the partial reduction given as parameter into this instance. This
     * method is used by the library when the reduction is being parallelized and
     * multiple instances of the implementing type obtained through method
     * {@link #newReducer()} have been used and the partial result of each of these
     * instances need to be combined into a single instance.
     *
     * @param reducer another instance of the implementing class whose partial
     *                reduction needs to be merged into this instance
     */
    public abstract void merge(R reducer);

    /**
     * Creates a new R instance and returns it. The returned element should be the
     * neutral element of the reduction operation being implemented, that is,
     * calling method {@link #merge(Reducer)} with the object returned by this
     * method without there being any call to {@link #reduce(Object)} should not
     * influence the result of the reduction, or throw any exception.
     *
     * @return a new R instance which is going to be used to parallelize the
     *         reduction
     */
    public abstract R newReducer();

    /**
     * Takes the object given as parameter and performs the reduction operation this
     * object implements.
     *
     * @param input an instance of the object on which the reduction operation is
     *              taking place
     */
    public abstract void reduce(T input);

    /**
     * This
     *
     * @param placeGroup into which this instance is participating
     */
    @SuppressWarnings("deprecation")
    public R teamReduction(TeamedPlaceGroup placeGroup) {
        if (placeGroup == null) {
            throw new IllegalStateException("This Reducer is not allowed to perform any global reduction");
        }

        // 1. We need to make sure that the reducer instance was registered in the
        // operations that MPI can handle
        Op mpiOperation;
        // The getCanonicalName method returns the name of the implementing class, not
        // "handist.collections.dist.Reducer".
        final String s = this.getClass().getCanonicalName();
        synchronized (registeredUserOperations) {
            if (!registeredUserOperations.containsKey(s)) {
                mpiOperation = new Op(this.newReducer(), true);
                registeredUserOperations.put(s, mpiOperation);
            } else {
                mpiOperation = registeredUserOperations.get(s);
            }
        }
        // 2. Prepare the local instances into an Object Array
        final Object[] buffer = new Object[1];
        buffer[0] = this;

        // Make the MPI AllReduce call
        placeGroup.comm.Allreduce(buffer, 0, buffer, 0, 1, MPI.OBJECT, mpiOperation);

        return (R) buffer[0];
    }
}
