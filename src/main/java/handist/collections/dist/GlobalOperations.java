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

import apgas.Constructs;
import apgas.Place;
import handist.collections.function.SerializableBiConsumer;

/**
 * Interface that defines the "Global Operations" that distributed
 * collections propose.
 * @param <C> implementing type, should be a class that implements 
 * {@link AbstractDistCollection}
 */
public abstract class GlobalOperations<C extends AbstractDistCollection<?>> {
	
	protected final C localHandle;
	
	GlobalOperations(C handle) {
		localHandle = handle;
	}
	
	/**
	 * Calls the provided action on the local instance of the distributed 
	 * collection on every place the collection is handled and returns.
	 * 
	 * @param action action to perform, the first parameter is the Place on 
	 * which the local instance is located, the second parameter is the local
	 * collection object
	 */
	public void onLocalHandleDo(SerializableBiConsumer<Place,C> action) {
		localHandle.placeGroup().broadcastFlat(()->{
			action.accept(Constructs.here(), localHandle);
		});
	}
	
	public void balance() {
		final TeamedPlaceGroup pg = localHandle.placeGroup();
		pg.broadcastFlat(() -> {
			localHandle.team().teamedBalance();
		});
	};
	
	public void balance(final float[] balance) {
		localHandle.balanceSpecCheck(balance);
		TeamedPlaceGroup pg = localHandle.placeGroup();
		pg.broadcastFlat(() -> {
			localHandle.team().teamedBalance(balance);
		});
	}
	
	/**
	 * Gathers the size of every local collection and returns it in the provided
	 * array
	 * @param result the array in which the result will be stored
	 */
	public void size(final long[] result) {
		localHandle.placeGroup().broadcastFlat(()->{
			localHandle.team().size(result);
		});
	}
	
}
