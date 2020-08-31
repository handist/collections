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

import apgas.Constructs;
import apgas.Place;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.MemberOfLazyObjectReference;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;

/**
 * Interface that defines the "Global Operations" that distributed
 * collections propose.
 * @param <T> the type of objects manipulated by the distributed collection
 * @param <C> implementing type, should be a class that implements 
 * {@link AbstractDistCollection}
 */
public abstract class GlobalOperations<T, C extends AbstractDistCollection<T,C>> implements SerializableWithReplace {
	
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
	 * Performs the specified action on every instance contained on every host
	 * of the distributed collection and returns when all operations have
	 * been completed. 
	 * <p>
	 * The action is performed on the 
	 * @param action action to perform
	 */
	public void forEach(final SerializableConsumer<T> action) {
		localHandle.placeGroup().broadcastFlat(()->{
			localHandle.forEach(action);
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
	
	/**
	 * Method used to create an object which will be transferred to a remote 
	 * place. 
	 * <p>
	 * This method is defined as <em>abstract</em> in class 
	 * {@link GlobalOperations} to force the implementation in child classes. 
	 * Implementation should return a {@link MemberOfLazyObjectReference}
	 * instance capable of initializing the local handle of member 
	 * {@link #localHandle} on the remote place and return the "GLOBAL" member
	 * of this handle's local class. 
	 * @return a {@link MemberOfLazyObjectReference} (left to programmer's 
	 * 	good-will) 
	 * @throws ObjectStreamException if such an exception is thrown during the
	 *  process
	 */
	public abstract Object writeReplace() throws ObjectStreamException;
}
