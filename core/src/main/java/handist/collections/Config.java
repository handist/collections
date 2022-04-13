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

/**
 * Some configuration elements concerning the library
 */
public interface Config {
    /**
     * Determines the maximum size of a {@link Chunk}.
     */
    public long maxChunkSize = Integer.MAX_VALUE;
    /**
     * When displaying the contents of a collection, determines how many elements
     * should be shown.
     */
    public int maxNumElementsToString = 10; // 10 is default
    /**
     * Determines if elements contained in a collection should be omitted when
     * displaying the contents with method {@code toString}. If set to {@code true},
     * {@link #maxNumElementsToString} determines how many should should be shown.
     */
    public boolean omitElementsToString = true; // true is default
}
