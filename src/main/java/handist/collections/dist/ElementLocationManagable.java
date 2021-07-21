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
public interface ElementLocationManagable<T> {
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
     * Conduct element location management process. This method must be called
     * simultaneously by process group members.
     */
    public void updateDist();
}
