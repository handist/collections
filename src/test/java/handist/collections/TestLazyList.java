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

import handist.collections.dist.Reducer;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static apgas.Constructs.finish;
import static org.junit.Assert.*;

@SuppressWarnings("deprecation")
public class TestLazyList {


    @Test
    public void test0() {
        Chunk<Long> chunk = new Chunk<Long>(new LongRange(10, 20), (Long index)->new Long(index));
        LazyRangedList<Long,Long> c2 = new LazyRangedList<>(chunk, (long index, Long a)->new Long(a*a));
        LazyRangedList<Long,Long> c3 = new LazyRangedList<>(chunk, (long index, Long a)->new Long(a*a*a));

        long index = 10;
        for(Long c: c2) {
            assertEquals(index*index, c.longValue());
            index++;
        }
        index = 12;
        Iterator<Long> i3 = c3.subIterator(new LongRange(12,18));
        while(i3.hasNext()) {
            assertEquals(index*index*index, i3.next().longValue());
            index++;
        }
        assertEquals(18, index);
    }

}
