/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.function;

import handist.collections.RangedList;

/**
 * Functional interface for actions taking a {@code long} and a generic type T 
 * as parameter. This is used to perform actions on {@link RangedList}s where 
 * the {@code long} index and the element are used.
 * <p>
 * This interface removes the boxing and un-boxing operations that would 
 * otherwise occur when using the standard 
 * {@link java.util.function.BiConsumer} with types {@link Long} and T as 
 * generic parameter types. 
 *
 * @param <T> type of the object used as second parameter
 */
public interface LongTBiConsumer<T> {
	/**
	 * Performs an action with the given {@code long} index and object of 
	 * generic type T.
	 *  
	 * @param l index of the object in the {@link RangedList}
	 * @param t object
	 */
    void accept(long l, T t);

    default LongTBiConsumer<T> andThen(LongTBiConsumer<? super T> after) {
        return new LongTBiConsumer<T>() {
            @Override
            public void accept(long l, T t) {
                this.accept(l, t);
                after.accept(l, t);
            }
        };
    }
}
