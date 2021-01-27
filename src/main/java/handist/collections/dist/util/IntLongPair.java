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
package handist.collections.dist.util;

/**
 * Simple class containing an integer ({@code int} and a {@code long}.
 */
public class IntLongPair {
    /** Integer as first member of this class */
    public int first;
    /** Long as second member of this class */
    public long second;

    /**
     * Constructor specifying the initial values of both members
     *
     * @param first  value for the integer
     * @param second value for the long
     */
    public IntLongPair(int first, long second) {
        this.first = first;
        this.second = second;
    }
}
