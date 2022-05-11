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
public interface ParallelReceiver<T> extends Iterable<T> {

    /**
     * Removes all contents from this parallel receiver. This instance will be empty
     * as a result.
     */
    void clear();

    /**
     * Indicates if the object given as parameter is present in this instance
     *
     * @param v the object whose presence is to be checked
     * @return {@code true} if the object is present at least once in this parallel
     *         receiver, {@code false} otherwise
     */
    boolean contains(Object v);

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

    /**
     * Indicates if this parallel receiver contains any object
     *
     * @return {@code true} if this instance is empty, {@code false} otherwise
     */
    boolean isEmpty();

    /**
     * Returns the size (i.e.) the number of elements contained in this instance.
     *
     * @return the number of elements contained in the receiver as an integer
     */
    int size();
}
