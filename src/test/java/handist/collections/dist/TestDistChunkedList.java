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
package handist.collections.dist;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.Chunk;
import handist.collections.LongRange;

public class TestDistChunkedList implements Serializable {

    /**
     * Generator function used to populate the values of the collections during
     * initialization
     */
    static final Function<Long, String> gen = (Long index) -> "xx" + index.toString();

    /** Serial Version UID */
    private static final long serialVersionUID = 5299598106800224984L;

    /**
     * Distributed collection of Strings containing 2 chunks:
     * <ul>
     * <li>[-10,-7)
     * <li>[10,100)
     * </ul>
     */
    DistChunkedList<String> distChunkedList;

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    /** Distributed collection of Strings initialized empty for the test */
    DistChunkedList<String> emptyDistChunkedList;

    /** World on which the DistCol under test is created (single-host world) */
    SinglePlaceGroup world;

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))) {
            System.err.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    @Before
    public void setup() {
        world = SinglePlaceGroup.getWorld();
        emptyDistChunkedList = new DistChunkedList<>(world);

        distChunkedList = new DistChunkedList<>(world);
        distChunkedList.add(new Chunk<>(new LongRange(10, 100), gen));
        distChunkedList.add(new Chunk<>(new LongRange(-10, -7), gen));
    }

    /**
     * Checks that the initialization with the "generator" function makes the
     * expected assignments.
     */
    @Test
    public void testConstructorWithGenerator() {
        final AtomicLong a = new AtomicLong(0);
        // Check that every mapped String has the expected value
        distChunkedList.forEach((long index, String e) -> {
            assertEquals(e, gen.apply(index));
            a.incrementAndGet();
        });
        // Check that the expected number of mappings is present
        assertEquals(a.get(), 93l);
        assertEquals(a.get(), distChunkedList.size());
    }
}
