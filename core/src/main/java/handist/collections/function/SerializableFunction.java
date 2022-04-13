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

import java.io.Serializable;
import java.util.function.Function;

/**
 * Serializable function
 *
 * @author Patrick
 *
 * @param <T1> input type of the function
 * @param <T2> return type of the function
 */
public interface SerializableFunction<T1, T2> extends Function<T1, T2>, Serializable {
}
