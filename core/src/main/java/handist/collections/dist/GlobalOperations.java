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
import java.util.function.BiFunction;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.MemberOfLazyObjectReference;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;

/**
 * Class that defines the "Global Operations" that distributed collections
 * propose.
 *
 * @param <T> the type of objects manipulated by the distributed collection
 * @param <C> implementing type, should be a class that implements
 *            {@link DistributedCollection}
 */
public class GlobalOperations<T, C extends DistributedCollection<T, C>> implements SerializableWithReplace {

    protected final C localHandle;
    protected final BiFunction<TeamedPlaceGroup, GlobalID, ? extends C> lazyCreator;

    /**
     *
     * @param handle      the target Distributed Collection
     * @param lazyCreator the way to create a branch of a distributed collection to
     *                    a new place.
     */
    GlobalOperations(C handle, BiFunction<TeamedPlaceGroup, GlobalID, ? extends C> lazyCreator) {
        localHandle = handle;
        this.lazyCreator = lazyCreator;
    }

    public void balance() {
        final TeamedPlaceGroup pg = localHandle.placeGroup();
        pg.broadcastFlat(() -> {
            localHandle.team().teamedBalance();
        });
    }

    public void balance(final float[] balance) {
        localHandle.balanceSpecCheck(balance);
        final TeamedPlaceGroup pg = localHandle.placeGroup();
        pg.broadcastFlat(() -> {
            localHandle.team().teamedBalance(balance);
        });
    }

    /**
     * Performs the specified action on every instance contained on every host of
     * the distributed collection and returns when all operations have been
     * completed.
     * <p>
     * The specified action is performed by a single thread on each host.
     *
     * @param action action to perform
     */
    public void forEach(final SerializableConsumer<T> action) {
        localHandle.placeGroup().broadcastFlat(() -> {
            localHandle.forEach(action);
        });
    };

    public void gather(final Place destination) {
        final TeamedPlaceGroup pg = localHandle.placeGroup();
        pg.broadcastFlat(() -> {
            localHandle.team().gather(destination);
        });
    }

    /**
     * Gathers the size of every local collection and returns it in the provided
     * array
     *
     * @param result the array in which the result will be stored
     */
    @SuppressWarnings("rawtypes")
    public void getSizeDistribution(final long[] result) {
        if (localHandle instanceof ElementLocationManageable) {
            ((ElementLocationManageable) localHandle).getSizeDistribution(result);
        } else {
            localHandle.placeGroup().broadcastFlat(() -> {
                localHandle.team().getSizeDistribution(result);
            });
        }
    }

    /**
     * Calls the provided action on the local instance of the distributed collection
     * on every place the collection is handled and returns.
     *
     * @param action action to perform, the first parameter is the Place on which
     *               the local instance is located, the second parameter is the
     *               local collection object
     */
    public void onLocalHandleDo(SerializableBiConsumer<Place, C> action) {
        localHandle.placeGroup().broadcastFlat(() -> {
            action.accept(Constructs.here(), localHandle);
        });
    }

    /**
     * Performs the specified action on every instance contained on every host of
     * the distributed collection and returns when all operations have been
     * completed.
     * <p>
     * The specified action is performed by multiple threads on each host.
     *
     * @param action action to perform
     */
    public void parallelForEach(final SerializableConsumer<T> action) {
        localHandle.placeGroup().broadcastFlat(() -> {
            localHandle.parallelForEach(action);
        });
    }

    /**
     * Method used to create an object which will be transferred to a remote place.
     * <p>
     * This method is defined as <em>abstract</em> in class {@link GlobalOperations}
     * to force the implementation in child classes. Implementation should return a
     * {@link MemberOfLazyObjectReference} instance capable of initializing the
     * local handle of the distributed collection on the remote place and return the
     * "GLOBAL" member of this handle's local class.
     *
     * @return a {@link MemberOfLazyObjectReference} (left to programmer's
     *         good-will)
     * @throws ObjectStreamException if such an exception is thrown during the
     *                               process
     */
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = localHandle.placeGroup();
        final GlobalID id1 = localHandle.id();
        return new MemberOfLazyObjectReference<>(pg1, id1, () -> {
            return lazyCreator.apply(pg1, id1);
        }, (handle) -> {
            return handle.global();
        });
    }
}
