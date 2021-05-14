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
 * Small utility class that keeps two objects in a pair
 *
 * @param <F> type of the first object
 * @param <S> type of the second object
 */
public class Pair<F, S> {
    /** first object of type F */
    public F first;
    /** second object of type S */
    public S second;

    /**
     * Constructor with the initial object values for the two objects contained in
     * this pair
     *
     * @param first  first object of type F
     * @param second second object of type S
     */
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public boolean equals(Object target) {
        if (!(target instanceof Pair)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        final Pair target0 = (Pair) target;
        return first.equals(target0.first) && second.equals(target0.second);
    }

    @Override
    public int hashCode() {
        return first.hashCode() << 2 + second.hashCode();
    }
}
