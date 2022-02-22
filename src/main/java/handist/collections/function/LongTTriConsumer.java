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
package handist.collections.function;

/**
 * Functional interface taking three arguments as parameter. The first parameter
 * is a {@code long} integer, the next two are generic type parameters
 *
 * @author Patrick Finnerty
 * @param <T> generic type for the second parameter
 * @param <U> generic type for the third parameter
 */
@FunctionalInterface
public interface LongTTriConsumer<T, U> {
    /**
     * Consumer of three arguments
     *
     * @param l long index
     * @param t instance of generic type T
     * @param u instance of generic type U
     */
    public void accept(long l, T t, U u);
}
