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

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;

public class TestLazyList {

    @Test
    public void test0() {
        final Chunk<Long> chunk = new Chunk<>(new LongRange(10, 20), (Long index) -> new Long(index));
        final LazyRangedList<Long, Long> c2 = new LazyRangedList<>(chunk, (long index, Long a) -> new Long(a * a));
        final LazyRangedList<Long, Long> c3 = new LazyRangedList<>(chunk, (long index, Long a) -> new Long(a * a * a));

        long index = 10;
        for (final Long c : c2) {
            assertEquals(index * index, c.longValue());
            index++;
        }
        index = 12;
        final Iterator<Long> i3 = c3.subIterator(new LongRange(12, 18));
        while (i3.hasNext()) {
            assertEquals(index * index * index, i3.next().longValue());
            index++;
        }
        assertEquals(18, index);
    }

}
