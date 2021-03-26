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
import java.util.function.BiConsumer;

/**
 * Serializable BiConsumer function. This interface used to represent
 * {@link BiConsumer}s that need to be serialized and transmitted to remote
 * hosts to be executed.
 *
 * @param <T> type of the first parameter
 * @param <U> type of the second parameter
 * @see SerializableConsumer
 */
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {
}
