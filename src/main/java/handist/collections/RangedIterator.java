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

import java.util.Iterator;
import java.util.ListIterator;

/**
 * Iterator interface for collections based on {@code long} indices. It allows
 * traveling the elements of the underlying collection in the forwarding direction.
 *
 * @param <T> type of objects on which this iterator operates
 */
public interface RangedIterator<T> extends Iterator<T> {

    /**
     * Returns the {@code long} index in {@link RangedList} of the element that
     * would be returned by calling method {@link #next()}
     *
     * @return the index of the element that calling {@link #next()} would return
     */
    public long nextIndex();

}
