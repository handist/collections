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

/**
 * The collection that implements this interface has feature to locate their
 * elements. For example, DistCol manages their elements by their index ranges
 * and DistIdMap manages their element by their keys.
 *
 * @param <T> The type of index or keys to manage the elements.
 */
public interface ElementLocationManageable<T> {
    /**
     * Computes and gathers the size of each local collection into the provided
     * array. In the case of {@code ElementLocationManagable}, this method is
     * conducted based on the information gathered by the previous
     * {@code updateDist()}.
     *
     * @param result the array in which the result will be stored
     */
    public void getSizeDistribution(final long[] result);

    /**
     * Registers a distribution object which needs to be updated by the distributed
     * collection. As the bare minimum, each time the {@link #updateDist()} method
     * is called, the distributed collection needs to ensure that the distribution
     * objects registered through this method are updated as well.
     * <p>
     * <h2>Implementation Note</h2> Distribution objects registered through this
     * method are weakly managed, meaning that registering a distribution object
     * into a distributed collection using this method will not prevent the garbage
     * collector from recollecting the distribution. In practice this does not
     * change anything for users. This simply means that registered distributions
     * that are no longer accessible from anywhere in the program will be
     * automatically dropped from the registered collections updated by the
     * distributed collection. Programmers need not to worry about "de-registering"
     * distribution objects when their use has expired as it will be done
     * automatically.
     *
     * @param distributionToUpdate the updatable distribution which needs to be
     *                             updated when the distribution of the distributed
     *                             collection evolves
     */
    public void registerDistribution(UpdatableDistribution<T> distributionToUpdate);

    /**
     * Update the local knowledge of each local handle so that it is up-to-date with
     * the entire distributed state of the distribution. This method needs to be
     * called on each local handle on which the distributed collection is defined.
     */
    public void updateDist();
}
