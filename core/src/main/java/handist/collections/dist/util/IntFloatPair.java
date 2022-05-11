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
 * Simple class containing an integer ({@code int} as "first member" and a
 * {@code float} as second member.
 */
public class IntFloatPair {
    /** integer fist member */
    public int first;
    /** float second member */
    public float second;

    /**
     * Constructor specifying the initial value for both members
     *
     * @param first  integer value for the first member
     * @param second float value for the second member
     */
    public IntFloatPair(int first, float second) {
        this.first = first;
        this.second = second;
    }
}
