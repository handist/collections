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
package handist.collections.glb;

import static apgas.Constructs.*;
import static handist.collections.glb.GlobalLoadBalancer.*;
import static handist.collections.glb.Util.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_GlbProgramTest1 implements Serializable {

    /** Number of ranges to populate this collection */
    final static long LONGRANGE_COUNT = 20l;

    /** Size of individual ranges */
    final static long RANGE_SIZE = 20l;

    /** Serial Version UID */
    private static final long serialVersionUID = -5017047700763986362L;

    /** Total number of elements contained in the {@link DistCol} */
    final static long TOTAL_DATA_SIZE = LONGRANGE_COUNT * RANGE_SIZE;

    /** DistCol used to test the GLB functionalities */
    DistCol<Element> col;

    /** PlaceGroup on which collection #map is defined */
    TeamedPlaceGroup placeGroup;

    /**
     * Subroutine used in several tests. This checks that the {@link #col}
     * distributed collection has not lost any single element, i.e. that it contains
     * {@value #TOTAL_DATA_SIZE} entries, possibly distributed across places.
     */
    private void checkNoElementLost() {
        // Check that no element of the map was lost
        final AtomicLong size = new AtomicLong(0);
        finish(() -> {
            for (final Place p : col.placeGroup().places()) {
                size.addAndGet(at(p, () -> {
                    return col.size();
                }));
            }
        });
        assertEquals(TOTAL_DATA_SIZE, size.get());
    }

    @Before
    public void setup() {
        placeGroup = TeamedPlaceGroup.getWorld();
        col = new DistCol<>(placeGroup);

        // Put some initial values in distMap
        for (long l = 0l; l < LONGRANGE_COUNT; l++) {
            final long from = l * RANGE_SIZE;
            final long to = from + RANGE_SIZE;
            final String lrPrefix = "LR[" + from + ";" + to + "]";
            final LongRange lr = new LongRange(from, to);
            final Chunk<Element> c = new Chunk<>(lr);
            for (long i = from; i < to; i++) {
                final String value = genRandStr(lrPrefix + ":" + i + "#");
                c.set(i, new Element(value));
            }
            col.add(c);
        }
    }

    @After
    public void tearDown() {
        col.destroy();
    }

    /**
     * This checks that the second operation only starts after the first one has
     * completed. If the contrary occurs, assertions will fail.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testAfter() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            final DistFuture<DistCol<Element>> test = col.GLB.forEach(makePrefixTest);
            col.GLB.forEach((e) -> assertTrue(e.s.startsWith("Test"))).after(test);
        });
        if (!ex.isEmpty()) {
            throw ex.get(0);
        }
        checkNoElementLost();
    }

    /**
     * This checks that the last operation only starts after the first two
     * completed. If the contrary occurs, assertions will fail.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testAfterTwo() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            //
            final DistFuture<DistCol<Element>> test = col.GLB.forEach(makePrefixTest);
            final DistFuture<DistCol<Element>> addZ = col.GLB.forEach(addZToPrefix).after(test);
            col.GLB.forEach((e) -> assertTrue(e.s.startsWith("ZTest"))).after(addZ);
        });
        if (!ex.isEmpty()) {
            throw ex.get(0);
        }
        checkNoElementLost();
    }

    /**
     * Same test as {@link #testAfterTwo()}, but using an alternative writing.
     * <p>
     * Here, the dependency of the third operation on the completion of the first
     * two operations is programmed explicitly. In practice this is redundant since
     * the second operation also depends on the completion of the first operation,
     * but it is better to explicitly check.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testAfterTwo_Alternative() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            final DistFuture<DistCol<Element>> test = col.GLB.forEach(makePrefixTest);
            final DistFuture<DistCol<Element>> addZ = col.GLB.forEach(addZToPrefix).after(test);
            col.GLB.forEach((e) -> assertTrue(e.s.startsWith("ZTest"))).after(test).after(addZ);
        });
        if (!ex.isEmpty()) {
            for (final Exception e : ex) {
                e.printStackTrace();
            }
            throw ex.get(0);
        }
        checkNoElementLost();
    }

    @Test(expected = IllegalStateException.class, timeout = 1000)
    public void testCallGlbMethodOutsideUnderGLB() throws Throwable {
        col.GLB.forEach(makePrefixTest);
    }

    /**
     * Checks that the {@link DistFuture#result()} method works as intended.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testDistFutureGetResult() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final DistCol<Element> result = col.GLB.forEach(addZToPrefix).result();
                assertEquals(result, col); // In the case of forEach, result is the same object

                // As the result is a blocking call, the checks will only be made after the
                // first forEach operation has completed
                result.GLB.forEach((e) -> assertTrue(e.s.startsWith("Z")));
            });
            if (!ex.isEmpty()) {
                throw ex.get(0);
            }

        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        checkNoElementLost();
    }

    @Test(timeout = 1000)
    public void testEmptyGlbProgram() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            ;
        });
        if (!ex.isEmpty()) {
            throw ex.get(0);
        }

    }

    /**
     * Checks the behavior of the GLB when an exception is directly thrown inside
     * the underGLB program
     *
     * @throws Throwable if thrown during the test
     */
    @Test(expected = RuntimeException.class, timeout = 1000)
    public void testIllegalStateUnderGlb() throws Throwable {
        final ArrayList<Exception> exc = underGLB(() -> {
            throw new RuntimeException("This runtime exception is part of a test, everything is fine.");
        });
        // We expect a single exception
        assertEquals(1, exc.size());

        // Throw the exception, the test will pass if the exception thrown is a
        // RuntimeException
        throw exc.get(0);
    }

    /**
     * Checks that a simple GLB program with a single instruction operates as
     * intended
     *
     * @throws Throwable if thrown during the program
     */
    @Test(timeout = 10000)
    public void testSingleOperationGlbProgram() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                col.GLB.forEach(makePrefixTest);
            });
            if (!ex.isEmpty()) {
                throw ex.get(0);
            }

        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        // Check that the instances held on each place have the correct prefix
        try {
            placeGroup.broadcastFlat(() -> {
                for (final Element e : col) {
                    assertTrue(e.s.startsWith("Test"));
                }
            });
        } catch (final MultipleException me) {
            for (final Throwable e : me.getSuppressed()) {
                e.printStackTrace();
            }
            throw me.getSuppressed()[0];
        }

        checkNoElementLost();
    }

    /**
     * Checks that the {@link DistFuture#waitGlobalTermination()} method correctly
     * triggers the computation of a single operation program. The GLB program being
     * tested here is equivalent to the one presented in
     * {@link #testSingleOperationGlbProgram()}.
     *
     * @throws Throwable if thrown during the program
     */
    @Test(timeout = 10000)
    public void testWaitGlobalTermination_RedundantWait() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                col.GLB.forEach(addZToPrefix).waitGlobalTermination();
            });
            if (!ex.isEmpty()) {
                throw ex.get(0);
            }

        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        checkNoElementLost();

        // Check that the instances held on each place have the correct prefix
        try {
            placeGroup.broadcastFlat(() -> {
                for (final Element e : col) {
                    // This checks that the operation "addZToPrefix" was not called twice
                    assertTrue(e.s.startsWith("Z"));
                }
            });
        } catch (final MultipleException me) {
            for (final Throwable e : me.getSuppressed()) {
                e.printStackTrace();
            }
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Checks that an operation is correctly submitted to the GLB after a
     * waitGlobalTermination call is made. Here, the check on the correct prefix is
     * made inside the same GLB program, rather than outside like
     * {@link #testWaitGlobalTermination_RedundantWait()}
     *
     * @throws Throwable if thrown during the program
     */
    @Test(timeout = 10000)
    public void testWaitGlobalTermination_WaitAsDependency() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                col.GLB.forEach(addZToPrefix).waitGlobalTermination();
                col.GLB.forEach((e) -> assertTrue(e.s.startsWith("Z")));
            });
            if (!ex.isEmpty()) {
                throw ex.get(0);
            }

        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        checkNoElementLost();
    }

    /**
     * Checks that a second call to wait global termination does not block anything
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testWaitGlobalTerminationRedundantCall() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final DistFuture<DistCol<Element>> future = col.GLB.forEach(addZToPrefix);
                future.waitGlobalTermination();
                future.waitGlobalTermination(); // This second call should not do anything
                final DistCol<Element> result = future.result(); // Result should already be available

                // As the result is a blocking call, the checks will only be made after the
                // first forEach operation has completed
                result.GLB.forEach((e) -> assertTrue(e.s.startsWith("ZLR[")));
            });
            if (!ex.isEmpty()) {
                throw ex.get(0);
            }

        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        checkNoElementLost();
    }

}
