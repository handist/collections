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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import handist.collections.Chunk;
import handist.collections.LongRange;

public class TestDistCol implements Serializable {

    /** Serial Version UID */
	private static final long serialVersionUID = 5299598106800224984L;
	
	/** World on which the DistCol under test is created (single-host world) */
	SinglePlaceGroup world;

	/** 
	 * Distributed collection of Strings containing 2 chunks: 
	 * <ul>
	 * <li>[-10,-7) 
	 * <li>[10,100) 
	 * </ul>
	 */
	DistCol<String> distCol;
	
	/** Distributed collection of Strings initialized empty for the test */
	DistCol<String> emptyDistCol;
	
	/** 
	 * Generator function used to populate the values of the collections during
	 * initialization
	 */
	static final Function<Long,String> gen = (Long index)-> "xx" + index.toString();
    
    @Before
    public void setup() {
        world = SinglePlaceGroup.getWorld();
    	emptyDistCol = new DistCol<String>(world);
    	
    	distCol = new DistCol<>(world);
        distCol.add(new Chunk<String>(new LongRange(10, 100), gen));
        distCol.add(new Chunk<String>(new LongRange(-10, -7), gen));
    }
    
    /**
     * Checks that the initialization with the "generator" function makes the
     * expected assignments.
     */
    @Test
    public void testConstructorWithGenerator() {
        AtomicLong a = new AtomicLong(0);
        // Check that every mapped String has the expected value
        distCol.forEach((long index, String e)->{
            assertEquals(e, gen.apply(index));
            a.incrementAndGet(); 
        });
        // Check that the expected number of mappings is present
        assertEquals(a.get(), 93l);
        assertEquals(a.get(), distCol.size());
    }
    
    @Test
    public void testProxyGenerator() {
        // Check that accessing index non present throws an error in the 
        // absence of a proxy
        assertThrows(IndexOutOfBoundsException.class, ()->{
            distCol.get(0l);
        });
        
        Function<Long,String> generatorForTest = (l)-> "oo" + l.toString();
        
        distCol.setProxyGenerator(generatorForTest);
        
        // Check on a range that intersects and overlaps distCol, that the proxy
        // set on DistCol generates values and that no exception is thrown
        new LongRange(-100l, 110l).forEach((long index)->{
        	if (distCol.containsIndex(index)) {
        		assertEquals("Problem at index " + index, distCol.get(index), gen.apply(index));        		
        	} else {
        		assertEquals("Problem at index " + index, distCol.get(index), generatorForTest.apply(index));
        	}
        });
    }
}
