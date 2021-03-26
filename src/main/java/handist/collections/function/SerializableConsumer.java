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
import java.util.function.Consumer;

/**
 * Serializable Consumer function. This interface represents a {@link Consumer}
 * function which can also be serialized. This is required in a number of
 * circumstances where lambda expressions need to be serialized in order to be
 * executed on remote hosts.
 *
 * @param <T> type consumed by the function
 * @see SerializableBiConsumer
 */
public interface SerializableConsumer<T> extends Consumer<T>, Serializable {
}
