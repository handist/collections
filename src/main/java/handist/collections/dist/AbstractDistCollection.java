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

import java.io.ObjectStreamException;
import java.util.ArrayList;

import apgas.util.GlobalID;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.function.SerializableConsumer;

/**
 * Interface representing the most basic characteristics of a distributed
 * collection
 *
 * @author Patrick Finnerty
 *
 * @param <T> type of the object contained in the distributed collection
 * @param <C> the underlying collection itself (used for reflective operations)
 */
public interface AbstractDistCollection<T, C extends AbstractDistCollection<T, C>> {

    static int _debug_level = 5;

    // @TransientInitExpr(getLocalData())

    // TODO keep these members in implementations
    // final GlobalID id;
    // public transient float[] locality;
    // public final TeamedPlaceGroup placeGroup; // may be packed into T? or
    // globalID??

    /*
     * Ensure calling updateDist() before balance() balance() should be called in
     * all places
     */

    public default void balanceSpecCheck(final float[] balance) {
        if (balance.length != placeGroup().size) {
            throw new RuntimeException("[AbstractDistCollection");
        }
    }

    /**
     * Destroy an instance of AbstractDistCollection.
     */
    public default void destroy() {
        placeGroup().remove(id());
    }

    /**
     * Performs the specified action on every instance contained by the local handle
     * of this distributed collection.
     *
     * @param action action to perform on each instance
     */
    public void forEach(SerializableConsumer<T> action);

    /**
     * Returns a handle to global operations of a distributed collection
     *
     * @return handle to "global" operations
     */
    public GlobalOperations<T, C> global();

    /**
     * Retuns the global ID that identifies this distributed collection on all hosts
     *
     * @return {@link GlobalID} of this distributed collection
     */
    public GlobalID id();

    /**
     * Places the local size of each local handle of the distributed object in the
     * provided array
     *
     * @param result the array inside which the result will be places, it should
     *               have the same length as the number of places in the place group
     *               on which the distributed collection operates
     */
    // this method is now implemented in GlobalOperations and TeamOperations
    // abstract public void distSize(long[] result);

    /**
     * Returns the locality of the distributed collection
     *
     * @return float array representing the locality of the distributed collection
     */
    public float[] locality();

    public abstract void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManagerLocal mm)
            throws Exception;

    /**
     * Performs the specified action on every instance contained by the local handle
     * of this distributed collection in parallel.
     *
     * @param action action to perform on each instance
     */
    public void parallelForEach(SerializableConsumer<T> action);

    // TODO
    // public abstract void integrate(T src);

    /**
     * Return the PlaceGroup on which this distributed collection was built
     *
     * @return PlaceGroup.
     */
    public TeamedPlaceGroup placeGroup();

    /**
     * Returns a handle to teamed operations of a distributed collection
     *
     * @return handle to "teamed" operations
     */
    public TeamOperations<T, C> team();

    /**
     * Method used to create an object which will be transferred to a remote place.
     * <p>
     * This method is present in interface {@link AbstractDistCollection} to force
     * the implementation in implementing classes. Implementation should return a
     * {@link LazyObjectReference} instance capable of initializing the local handle
     * of the implementing class on the remote place
     *
     * @return a {@link LazyObjectReference} of the implementing class (left to
     *         programmer's good-will)
     * @throws ObjectStreamException if such an exception is thrown during the
     *                               process
     */
    abstract public Object writeReplace() throws ObjectStreamException;

    /*
     * public final def printAllData(){ for(p in placeGroup){ at(p){
     * printLocalData(); } } }
     */

}
