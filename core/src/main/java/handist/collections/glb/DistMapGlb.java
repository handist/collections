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

import handist.collections.dist.DistMap;
import handist.collections.function.SerializableConsumer;

/**
 *
 * This class proposes a number of operations that can be performed on a
 * {@link DistMap} as part of a GLB program. All the methods proposed by this
 * class should be called as part of a
 * {@link GlobalLoadBalancer#underGLB(apgas.SerializableJob)} call.
 *
 * @author Patrick Finnerty
 *
 * @param <K> class used as key for the {@link DistMap}
 * @param <V> class used as value for the {@link DistMap}
 */
public class DistMapGlb<K, V> extends AbstractGlbHandle implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 5338165419227692355L;

    /** map handle on which this {@link DistMapGlb} operates */
    DistMap<K, V> map;

    /**
     * Constructor for the GLB operations handle of {@link DistMap}.
     *
     * @param m the underlying map on which operations will be made
     */
    public DistMapGlb(DistMap<K, V> m) {
        map = m;
    }

    /**
     * Submits an action to be performed on each key contained by the distributed
     * map.
     *
     * @param action action to be performed on each key of the {@link DistMap}
     * @return a DistFuture representing this operation
     * @throws IllegalStateException if this method is called outside of a
     *                               {@link GlobalLoadBalancer#underGLB(apgas.SerializableJob)}
     *                               method
     */
    public GlbFuture<DistMap<K, V>> forEach(SerializableConsumer<V> action) {
//		final GlobalLoadBalancer glb = getGlb();
//
//		// Submit the operation to the GLB
//		@SuppressWarnings("unlikely-arg-type")
//		final GlbOperation<DistMap<K, V>, V, K, DistMap<K, V>> operation = new GlbOperation<>(map, // This operation
//				// acts on map
//				(k) -> {
//					action.accept(map.get(k));
//				}, // Action consists in applying the action for each value (values are obtained by
//					// calling get(key) on the map)
//				() -> {
//					return new ArrayList<>(map.keySet());
//				}, // The keys (input for above action) are obtained by calling keySet on map
//				new DistFuture<>(map), // The result of this operation is a DistFuture<DistMap<K, V>>
//				null);
//		glb.submit(operation);
//
//		// Return the DistFuture representing the operation that was just submitted
//		return operation.future;
        return null;
    }
}
