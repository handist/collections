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
import java.util.function.Supplier;

/**
 * Serializable implementation of the standard Java {@link Supplier} interface.
 *
 * @param <T> type provided by this serializable supplier
 */
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
