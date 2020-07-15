/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import handist.collections.Chunk;
import handist.collections.LongRange;

public class TestDistCol implements Serializable {

    SinglePlaceGroup world;

    @Before
    public void setup() {
        world = SinglePlaceGroup.getWorld();
    }
    
    // TODO move to single node test after preparing world setup for single node.
    @Test
    public void proxyGenerator() {
        DistCol<String> distCol = new DistCol<String>(world);
        Function<Long,String> gen = (Long index)-> {
            return "xx"+ index.toString();
        };
        distCol.addChunk(new Chunk<String>(new LongRange(10, 100), gen));
        distCol.addChunk(new Chunk<String>(new LongRange(-10, -7), gen));
        AtomicInteger a = new AtomicInteger(0);
        distCol.forEach((long index, String e)->{
            assertEquals(e, gen.apply(index));
            a.incrementAndGet();
        });
        assertEquals(a.get(), 93);
        assertThrows(IndexOutOfBoundsException.class, ()->{
            distCol.get(0L);
        });
        
        distCol.setProxyGenerator(gen);
        new LongRange(-100, 110).forEach((long index)->{
            assertEquals(distCol.get(index), gen.apply(index));
        });
    }
}
