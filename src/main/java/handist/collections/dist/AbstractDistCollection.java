/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import java.io.ObjectStreamException;
import java.util.ArrayList;

import apgas.util.GlobalID;
import handist.collections.dist.util.IntLongPair;


public interface AbstractDistCollection<C extends AbstractDistCollection<C>> {

	static int _debug_level = 5;

	// @TransientInitExpr(getLocalData())

	//TODO keep these members in implementations
	// final GlobalID id;
	// public transient float[] locality;
	// public final TeamedPlaceGroup placeGroup; // may be packed into T? or globalID??

	/*
	 * Ensure calling updateDist() before balance() balance() should be called in
	 * all places
	 */

	/**
	 * Returns the locality of the distributed collection
	 * @return float array representing the locality of the distributed 
	 * collection
	 */
	public float[] locality();

	/**
	 * Retuns the global ID that identifies this distributed collection on all 
	 * hosts
	 * @return {@link GlobalID} of this distributed collection
	 */
	public GlobalID id();
	
	public default void balanceSpecCheck(final float[] balance) {
		if (balance.length != placeGroup().size) {
			throw new RuntimeException("[AbstractDistCollection");
		}        
	}
	

	/**
	 * Returns a handle to teamed operations of a distributed collection
	 * @return handle to "teamed" operations
	 */
	public TeamOperations<C> team();
	/**
	 * Returns a handle to global operations of a distributed collection
	 * @return handle to "global" operations
	 */
	public GlobalOperations<C> global();

	/**
	 * Places the local size of each local handle of the distributed object
	 * in the provided array
	 * @param result the array inside which the result will be places, it should
	 * have the same length as the number of places in the place group on which
	 * the distributed collection operates
	 */
	// this method is now implemented in GlobalOperations and TeamOperations
	//abstract public void distSize(long[] result);

	/**
	 * Destroy an instance of AbstractDistCollection.
	 */
	public default void destroy() {
		placeGroup().remove(id());
	}

	public abstract void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManagerLocal mm)
			throws Exception;

	/**
	 * Return the PlaceGroup on which this distributed collection was built
	 *
	 * @return PlaceGroup.
	 */
	public TeamedPlaceGroup placeGroup(); /*{
		return placeGroup;
	}*/

	// TODO
	// public abstract void integrate(T src);
	

	abstract public Object writeReplace() throws ObjectStreamException;
	// return new LaObjectReference(id, ()->{ new AbstractDistCollection<>());

	/*
    public final def printAllData(){
        for(p in placeGroup){
            at(p){
                printLocalData();
            }
        }
    }*/

}
