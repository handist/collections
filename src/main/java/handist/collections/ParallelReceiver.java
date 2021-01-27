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
package handist.collections;

import java.util.function.Consumer;

/**
 * Interface representing the capability for an implementing class to receive
 * objects of a certain type from multiple sources. The unique method should
 * provide a clean handle to feed T objects into the implementing type, without
 * interfering with concurrent threads using other handles to do the same.
 *
 * @param <T> type of the objects to receive
 */
public interface ParallelReceiver<T> {
    /**
     * Provides a new handle that will place T instances into this instance. This
     * method may be called by multiple threads that all want to place some
     * instances into this object. The implementation guarantees that all these
     * threads will be able to place T instances in this object through the returned
     * {@link Consumer} without blocking or otherwise contend for object access with
     * other threads.
     *
     * @return a new {@link Consumer} accepting T objects
     */
    Consumer<T> getReceiver();
}
