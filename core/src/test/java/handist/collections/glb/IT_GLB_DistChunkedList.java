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
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.CollectiveMoveManager;
import handist.collections.dist.DistBag;
import handist.collections.dist.DistChunkedList;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.reducer.Reducer;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_GLB_DistChunkedList implements Serializable {

    /**
     * Dummy reduction implementation which counts the number of instances on a
     * collection
     *
     * @author Patrick Finnerty
     *
     */
    private static class SumReduction extends Reducer<SumReduction, Element> {

        /** Serial Version UID */
        private static final long serialVersionUID = 757110980542818596L;
        long runningSum;

        private SumReduction() {
            runningSum = 0l;
        }

        @Override
        public void merge(SumReduction reducer) {
            runningSum += reducer.runningSum;
        }

        @Override
        public SumReduction newReducer() {
            return new SumReduction();
        }

        @Override
        public void reduce(Element input) {
            runningSum++;
        }

        @Override
        public String toString() {
            return "SumReduction: " + runningSum;
        }
    }

    /** Number of ranges to populate this collection */
    final static long LONGRANGE_COUNT = 20l;

    /** Number of hosts on which this tests runs */
    final static int PLACEGROUP_SIZE = 4;

    /** Size of individual ranges */
    final static long RANGE_SIZE = 4 * PLACEGROUP_SIZE + 1;

    /** Serial Version UID */
    private static final long serialVersionUID = 3890454865986201964L;

    /** Total number of elements contained in the {@link DistChunkedList} */
    final static long TOTAL_DATA_SIZE = LONGRANGE_COUNT * RANGE_SIZE;

    private static <T> void y_makeDistribution(DistChunkedList<T> col) {
        // Transfer elements to remote hosts (Places 0, 1, 2 - 3 doesn't get any)
        final TeamedPlaceGroup pg = col.placeGroup();
        pg.broadcastFlat(() -> {
            final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
            col.forEachChunk((RangedList<T> c) -> {
                final LongRange r = c.getRange();
                // We place the chunks everywhere except on place 3
                final Place destination = place((int) r.from % (PLACEGROUP_SIZE - 1));
                col.moveRangeAtSync(r, destination, mm);
            });
            mm.sync();
        });
    }

    /**
     * Helper method which fill the provided DistCol with values
     *
     * @param col the collection which needs to be populated
     */
    private static void y_populateDistCol(DistChunkedList<Element> col) {
        for (long l = 0l; l < LONGRANGE_COUNT; l++) {
            final long from = l * RANGE_SIZE;
            final long to = from + RANGE_SIZE;
            final String lrPrefix = "LR[" + from + ";" + to + "]";
            final LongRange lr = new LongRange(from, to);
            final Chunk<Element> c = new Chunk<>(lr);
            for (long i = from; i < to; i++) {
                final String value = lrPrefix + ":" + i + "#";
                c.set(i, new Element(value));
            }
            col.add(c);
        }
    }

    /**
     * Small check on the given {@link DistBag} that the number of lists contained
     * is equal to the provided integer.
     *
     * @param bag           {@link DistBag} to check
     * @param expectedCount expected number of lists internally contained in each
     *                      local handle of the provided {@link DistBag}
     * @throws Throwable if thrown as part of this small procedure
     */
    private static void z_checkBagNumberOfLists(DistBag<Element> bag, int expectedCount) throws Throwable {
        for (final Place p : bag.placeGroup().places()) {
            at(p, () -> {
                assertEquals(expectedCount, bag.listCount());
            });
        }
    }

    /**
     * Checks that the given {@link DistBag} holds the specified number of objects
     * distributed across all the places on which it is defined.
     *
     * @param bag           DistBag whose size needs to be checked
     * @param expectedTotal expected number of objects
     * @throws Throwable if thrown as part of this small test procedure
     */
    private static void z_checkBagTotalElements(DistBag<Element> bag, long expectedTotal) throws Throwable {
        long count = 0;
        for (final Place p : bag.placeGroup().places()) {
            count += at(p, () -> {
                return bag.size();
            });
        }
        assertEquals(expectedTotal, count);
    }

    /**
     * Checks that the distCol contains exactly the specified number of entries. The
     * {@link DistChunkedList#size()} needs to match the specified parameter.
     *
     * @param expectedCount expected total number of entries in
     *                      {@link #distChunkedList}
     * @throws Throwable if thrown during the check
     */
    private static void z_checkDistColTotalElements(DistChunkedList<Element> col, long expectedCount) throws Throwable {
        long count = 0;
        for (final Place p : col.placeGroup().places()) {
            count += at(p, () -> {
                return col.size();
            });
        }
        assertEquals(expectedCount, count);
    }

    /**
     * Checks that the prefix of each element in {@link #distChunkedList} is the one
     * specified
     *
     * @param prefix expected prefix
     * @throws Throwable if thrown during the check
     */
    private static void z_checkPrefixIs(DistChunkedList<Element> col, final String prefix) throws Throwable {
        try {
            col.GLOBAL.forEach((e) -> assertTrue("String was " + e.s + " when it should have started with " + prefix,
                    e.s.startsWith(prefix)));
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
    }

    /**
     * Checks the suffix of every element contained in the provided DistBag
     *
     * @param bag    DistBag whose elements need to be checked
     * @param suffix suffix expected to be on every element
     * @throws Throwable if thrown as part of this small procedure
     */
    private static void z_checkSuffixIs(DistBag<Element> bag, final String suffix) throws Throwable {
        try {
            bag.GLOBAL.forEach((e) -> assertTrue("String was " + e.s, e.s.endsWith(suffix)));
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
    }

    /**
     * Checks that the suffix of every element in the collection is the one
     * specified
     *
     * @param suffix string which should be at the end of each element
     * @throws Throwable of throw during the test
     */
    private static void z_checkSuffixIs(DistChunkedList<Element> col, final String suffix) throws Throwable {
        try {
            col.GLOBAL
                    .forEach((e) -> assertTrue("String was " + e.s + " when it should have ended with String:" + suffix,
                            e.s.endsWith(suffix)));
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
    }

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    /**
     * Distributed collection which is the object of the tests. It is defined on the
     * entire world.
     */
    DistChunkedList<Element> distChunkedList;

    /**
     * Whole world
     */
    TeamedPlaceGroup placeGroup;

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))
                && DebugFinish.suppressedExceptionsPresent()) {
            System.err.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        distChunkedList = new DistChunkedList<>();

        y_populateDistCol(distChunkedList);
        y_makeDistribution(distChunkedList);
    }

    @After
    public void tearDown() throws Exception {
        distChunkedList.destroy();
        GlbComputer.destroyGlbComputer();
    }

    /**
     * Checks that there is enough parallelism on all hosts participating in the
     * test to allow some concurrency between the workers of the Global Load
     * Balancer. This test throws an error rather than failing on an assertion to
     * emphasize the importance of such a failure.
     *
     * @throws Exception if there is not enough parallelism on the host
     */
    @Test(timeout = 10000)
    public void testEnvironmentHasEnoughParallelism() throws Exception {
        for (final Place p : places()) {
            at(p, () -> {
                if (Runtime.getRuntime().availableProcessors() <= 1) {
                    throw new Exception("Not Enough Parallelism To Run These Tests");
                }
            });
        }
    }

    /**
     * Checks the
     * {@link DistColGlb#forEach(handist.collections.function.SerializableConsumer)}
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 20000)
    public void testForEach() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                distChunkedList.GLB.forEach(makePrefixTest);
            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
        z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE);
        z_checkPrefixIs(distChunkedList, "Test");
    }

    /**
     * Checks the
     * {@link DistColGlb#forEach(handist.collections.function.SerializableLongTBiConsumer)}
     * function
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 20000)
    public void testForEachLongTBiconsumer() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                // Check that no error is thrown during the GLB operation
                assertTrue(distChunkedList.GLB.forEach((l, e) -> {
                    assertTrue(e.s.endsWith(l + "#"));
                }).getErrors().isEmpty());
            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
        z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE);
    }

    /**
     * This test checks that the map function of the {@link DistColGlb} handle
     * produces the expected results. It is currently ignored due to a pending
     * problem in the implementation.
     *
     * @throws Throwable if thrown during the test
     * @see DistColGlb#map(handist.collections.function.SerializableFunction)
     */
    @Test(timeout = 20000)
    public void testMap() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final DistChunkedList<Element> result = distChunkedList.GLB.map((e) -> {
                    return new Element(e.s + "Test");
                }).result();

                try {
                    z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE); // This shouldn't have changed
                    z_checkDistColTotalElements(result, TOTAL_DATA_SIZE); // Should contain the same number of elements
                    z_checkSuffixIs(result, "Test"); // The elements contained in the result should have 'Test' as
                    // prefix
                } catch (final Throwable e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        } catch (final RuntimeException re) {
            if (re.getCause() instanceof AssertionError) {
                throw re.getCause();
            } else {
                throw re;
            }
        }
    }

    @Ignore
    @Test(timeout = 10000)
    public void testReduction() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final SumReduction red = new SumReduction();

                final SumReduction result = distChunkedList.GLB.reduce(red).result();
                assertEquals(red, result);
                assertEquals(TOTAL_DATA_SIZE, result.runningSum);
            });
            if (!ex.isEmpty()) {
                throw new RuntimeException(ex.get(0));
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        } catch (final RuntimeException re) {
            if (re.getCause() instanceof AssertionError) {
                throw re.getCause();
            } else {
                throw re;
            }
        }
    }

    /**
     * Checks that the correct number of elements was created as well as the
     * distribution of these instances
     */
    @Test(timeout = 10000)
    public void testSetup() {
        long total = 0;
        for (final Place p : distChunkedList.placeGroup().places()) {
            total += at(p, () -> {
                if (p.id != 3) {
                    assertTrue(distChunkedList.numChunks() > 0);
                } else {
                    assertTrue(distChunkedList.numChunks() == 0);
                }
                return distChunkedList.size();
            });
        }
        assertEquals(TOTAL_DATA_SIZE, total);
    }

    /**
     * Checks that the "toBag" operation of DistCol. This operation relies on the
     * WorkerService functionalities to bind a dedicated List to each worker.
     *
     * @throws Throwable if thrown during the computation
     */
    @Test(timeout = 20000)
    public void testToBag() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final DistBag<Element> result = distChunkedList.GLB.toBag((e) -> {
                    return new Element(e.s + "Test");
                }).result();

                try {
                    z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE); // This shouldn't have changed
                    // There should be as many lists in the handles of the DistBag as there are
                    // workers on the hosts
                    z_checkBagNumberOfLists(result, GlbComputer.getComputer().MAX_WORKERS);
                    z_checkBagTotalElements(result, TOTAL_DATA_SIZE); // Should contain the same number of elements
                    z_checkSuffixIs(result, "Test"); // The elements contained in the result should have 'Test' as
                    // prefix
                } catch (final Throwable e) {
                    throw new RuntimeException(e);
                }

            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        } catch (final RuntimeException re) {
            if (re.getCause() instanceof AssertionError) {
                throw re.getCause();
            } else {
                throw re;
            }
        }
    }

    /**
     * Checks
     * {@link DistColGlb#toBag(handist.collections.function.SerializableBiConsumer, DistBag)}
     * method
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 60000)
    public void testToBagVariant() throws Throwable {
        final DistBag<Element> resultBag = new DistBag<>();
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final DistBag<Element> result = distChunkedList.GLB.toBag((e, r) -> {
                    final Element ele = new Element(e.s + "Test");
                    r.accept(ele);
                }, resultBag).result();

                try {
                    assertEquals(result, resultBag);
                    z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE); // This shouldn't have changed
                    // There should be as many lists in the handles of the DistBag as there are
                    // workers on the hosts
                    z_checkBagNumberOfLists(result, GlbComputer.getComputer().MAX_WORKERS);
                    z_checkBagTotalElements(result, TOTAL_DATA_SIZE); // Should contain the same number of elements
                    z_checkSuffixIs(result, "Test"); // The elements contained in the result should have 'Test' as
                    // prefix
                } catch (final Throwable e) {
                    throw new RuntimeException(e);
                }

            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        } catch (final RuntimeException re) {
            if (re.getCause() instanceof AssertionError) {
                throw re.getCause();
            } else {
                throw re;
            }
        }
    }

    @Test(timeout = 60000)
    public void testTwoConcurrentForEach() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                distChunkedList.GLB.forEach(makePrefixTest);
                distChunkedList.GLB.forEach(makeSuffixTest);
            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
        z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE);
        z_checkPrefixIs(distChunkedList, "Test");
        z_checkSuffixIs(distChunkedList, "Test");
    }

    @Test(timeout = 20000)
    public void testTwoDifferentCollectionsComputations() throws Throwable {
        final DistChunkedList<Element> otherCol = new DistChunkedList<>();
        y_populateDistCol(otherCol);
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                distChunkedList.GLB.forEach(makeSuffixTest);
                otherCol.GLB.forEach(makePrefixTest);
            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE);
        z_checkDistColTotalElements(otherCol, TOTAL_DATA_SIZE);
        z_checkSuffixIs(distChunkedList, "Test");
        z_checkPrefixIs(otherCol, "Test");

        otherCol.destroy();
    }

    @Test(timeout = 15000)
    public void testTwoForEachAfterOneAnother() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final GlbFuture<?> prefixFuture = distChunkedList.GLB.forEach(makePrefixTest);
                distChunkedList.GLB.forEach(makeSuffixTest).after(prefixFuture);
            });
            if (!ex.isEmpty()) {
                ex.get(0).printStackTrace();
                throw ex.get(0);
            }
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
        z_checkDistColTotalElements(distChunkedList, TOTAL_DATA_SIZE);
        z_checkPrefixIs(distChunkedList, "Test");
        z_checkSuffixIs(distChunkedList, "Test");
    }
}
