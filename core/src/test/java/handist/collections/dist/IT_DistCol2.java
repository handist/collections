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

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
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
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Second class used to test the distributed features of class
 * {@link DistChunkedList}
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistCol2 implements Serializable {

    /**
     * Helper object to generate values to populate the collection
     */
    static Random random;

    /** Serial Version UID */
    private static final long serialVersionUID = -8191119224439969307L;

    /**
     * Helper method which fills the given chunk with random values
     *
     * @param c      the chunk to fill with values
     * @param prefix the prefix to all the String value inserted
     */
    static void fillWithValues(Chunk<Element> c, String prefix) {
        c.forEach((index, s) -> {
            final Element e = new Element(prefix + random.nextInt(1000));
            c.set(index, e);
        });
    }

    /**
     * Initializes the static objects of the class
     */
    @BeforeClass
    public static void setupBeforeClass() {
        random = new Random(12345l);
    }

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    /** First chunk contained by the DistCol */
    Chunk<Element> chunk0To100;

    /** Second chunk contained by the DistCol */
    Chunk<Element> chunk100To200;
    /** Third chunk contained by the DistCol */
    Chunk<Element> chunk200To250;
    /** Instance used under test */
    DistCol<Element> distCol;

    /** First range on which DistCol is defined */
    LongRange range0To100;

    /** Second range on which DistCol is defined */
    LongRange range100To200;
    /** Third range on which DistCol is defined */
    LongRange range200To250;
    /** TeamedPlaceGroup representing the whole world */
    TeamedPlaceGroup world;

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

    /**
     * Utility method used to determine the "next" host in a rotation
     *
     * @return the "next" place by rotation
     */
    private Place nextPlaceRotate() {
        final int placeId = here().id;
        final int next = placeId + 1 < world.size() ? placeId + 1 : 0;
        return world.get(next);
    }

    /**
     * Prepares the various objects used for the tests
     *
     * @throws Exception if thrown during setup
     */
    @Before
    public void setUp() throws Exception {
        world = TeamedPlaceGroup.getWorld();

        range0To100 = new LongRange(0l, 100l);
        range100To200 = new LongRange(100l, 200l);
        range200To250 = new LongRange(200l, 250l);

        chunk0To100 = new Chunk<>(range0To100);
        chunk100To200 = new Chunk<>(range100To200);
        chunk200To250 = new Chunk<>(range200To250);

        fillWithValues(chunk0To100, "a");
        fillWithValues(chunk100To200, "b");
        fillWithValues(chunk200To250, "c");

        distCol = new DistCol<>(world);
    }

    /**
     * Destroys the DistCol instance used for the tests
     *
     * @throws Exception is thrown during destruction
     */
    @After
    public void tearDown() throws Exception {
        distCol.destroy();
    }

    /**
     * Checks that adding a chunk in a place makes remote handles aware of the
     * change
     *
     * @throws Throwable if such a throwable is thrown during the test
     */
    @Test(timeout = 10000)
    public void testAddUpdatesDistributedInformation() throws Throwable {
        distCol.add(chunk0To100); // Add a chunk to local handle of place 0
        distCol.placeGroup().broadcastFlat(() -> {
            final long[] size = new long[world.size()];

            distCol.TEAM.updateDist(); // Here is the important call
            distCol.TEAM.getSizeDistribution(size); // We check the result of TEAM.size

            assertEquals(world.size(), size.length);
            for (int i = 0; i < size.length; i++) {
                final long expectedSize = i == 0 ? 100l : 0l;
                assertEquals(expectedSize, size[i]);
            }
        });
    }

    /**
     * Checks that the global forEach operates as intended
     *
     * @throws Throwable if such a throwable is thrown during the test
     */
    @Test(timeout = 10000)
    public void testAlternativeGlobalForEach() throws Throwable {
        try {
            // Place chunks in different handles
            distCol.placeGroup().broadcastFlat(() -> {
                switch (here().id) {
                case 0:
                    distCol.add(chunk0To100);
                    break;
                case 1:
                    distCol.add(chunk100To200);
                    break;
                case 2:
                    distCol.add(chunk200To250);
                    break;
                }
            });

            // Make a manual forEach
            // distCol.GLOBAL.forEach((s)->{
            // System.out.println(s); // = "testGlobal" + s;
            // });

            distCol.placeGroup().broadcastFlat(() -> {
                distCol.forEach((e) -> {
                    e.s = "testGlobal" + e.s;
                });
            });

            // Check that every string was modified

            distCol.placeGroup().broadcastFlat(() -> {
                for (final Element e : distCol) {
                    assertTrue(e.s.startsWith("testGlobal"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Checks that the {@link LongRangeDistribution} returned by a {@link DistCol}
     * works as expected
     */
    @Test
    public void testDistributionSnapshots() {
        // Before any insertion, is empty
        final LongRangeDistribution initialDistribution = distCol.getDistribution();
        assertTrue(initialDistribution.getDistribution().isEmpty());

        // Insert some local and remote values
        distCol.add(chunk0To100);
        at(place(1), () -> {
            distCol.add(chunk100To200);
        });

        final LongRangeDistribution afterInsert = distCol.getDistribution();
        assertTrue(!afterInsert.getDistribution().isEmpty());
        final Map<LongRange, Place> mappings = afterInsert.rangeLocation(range0To100);
        assertEquals(place(0), mappings.get(range0To100));
    }

    @Test
    public void testDistributionSubscribed() {
        final LongRangeDistribution distribution = new LongRangeDistribution();
        distCol.registerDistribution(distribution);

        assertNull(distribution.location(range0To100));
        assertNull(distribution.location(range100To200));
        assertNull(distribution.location(range200To250));

        at(place(1), () -> distCol.add(chunk0To100));
        distCol.add(chunk100To200);
        // Remote modification are not yet known on Place0
        assertEquals(place(0), distribution.location(range100To200));
        assertNull(distribution.location(range0To100));

        world.broadcastFlat(() -> distCol.updateDist());

        // After updateDist, everybody knows everybody
        assertEquals(place(1), distribution.location(range0To100));
        assertEquals(place(1), distribution.location(52l));

        // Move a specific range from place(0) to place(1)
        final LongRange movedRange = new LongRange(70, 80);
        world.broadcastFlat(() -> {
            final CollectiveMoveManager mm = new CollectiveMoveManager(world);
            distCol.moveRangeAtSync(movedRange, place(0), mm);
            mm.sync();

            if (here() == place(0)) {
                System.err.println("About to call update procedure");
            }
            world.barrier();

            distCol.updateDist();
        });

        for (final Place p : world.places) {
            if (p == here()) {
                for (final long l : range0To100) {
                    final Place expected = movedRange.contains(l) ? place(0) : place(1);

                    assertEquals("Index " + l + " is mislocated on " + p, expected, distribution.location(l));
                }
            } else {
                at(p, () -> {
                    // Check that every single element's location can be accessed
                    final LongRangeDistribution remotedistrib = distCol.getDistribution();
                    for (final long l : range0To100) {
                        final Place expected = movedRange.contains(l) ? place(0) : place(1);
                        assertEquals("Index " + l + " is mislocated on " + p, expected, remotedistrib.location(l));
                    }
                });
            }
        }

        distCol.clear();

        // After clearing the local contents, remote status should remain known.
        assertTrue(!distCol.contains(range100To200)); // this chunk was just removed
        assertNull(distribution.location(range100To200));
        assertEquals(place(0), at(place(1), () -> {
            return distCol.getDistribution().location(range100To200);
        }));
        assertNull(distribution.location(range0To100)); // this range was split, hence no mapping

        world.broadcastFlat(() -> distCol.updateDist());

        assertNull(distribution.location(range100To200));

        // After the update, getting a snapshot of the distribution on place(1) to check
        at(place(1), () -> {
            final LongRangeDistribution distSnapshot = distCol.getDistribution();
            assertNull(distSnapshot.location(range0To100));
            assertEquals(place(1), distSnapshot.location(new LongRange(0, 70)));
            assertNull(distSnapshot.location(range100To200));
        });
    }

    /**
     * Checks that registered {@link LongRangeDistribution} get updated with correct
     * information. This test specifically uses a rotation between 4 places to make
     * sure that no confusion is made between ranks and places.
     */
    @Test
    public void testDistributionSubscribed2() {
        world.broadcastFlat(() -> {
            final LongRangeDistribution lrd = new LongRangeDistribution();
            distCol.registerDistribution(lrd);
            switch (here().id) {
            case 0:
                distCol.add(chunk0To100);
                break;
            case 1:
                distCol.add(chunk100To200);
                break;
            case 2:
                distCol.add(chunk200To250);
                break;
            default:
            }

            // Perform a rotation
            final CollectiveMoveManager mm = new CollectiveMoveManager(world);
            final RangedDistribution<LongRange> dist = lr -> {
                final Map<LongRange, Place> toReturn = new HashMap<>();
                toReturn.put(lr, nextPlaceRotate());
                return toReturn;
            };
            distCol.moveRangeAtSync(dist, mm);
            mm.sync();

            // Update distribution
            distCol.updateDist();

            // Debug print
            for (final LongRange lr : distCol.ranges()) {
                System.out.println(here() + ":rank" + world.rank() + " contains " + lr);
            }

            // Check contents of the lrd
            assertEquals(world.get(1), lrd.location(range0To100));
            assertEquals(world.get(2), lrd.location(range100To200));
            assertEquals(world.get(3), lrd.location(range200To250));
        });
    }

    /**
     * Checks that registered {@link LongRangeDistribution} get updated with correct
     * information. This test specifically uses a rotation between 4 places to make
     * sure that no confusion is made between ranks and places.
     */
    @Test
    public void testDistributionSubscribed3() {
        world.broadcastFlat(() -> {
            final LongRangeDistribution lrd = new LongRangeDistribution();
            distCol.registerDistribution(lrd);
            // Add ranges and perform rotation
            final CollectiveMoveManager mm = new CollectiveMoveManager(world);
            switch (here().id) {
            case 0:
                distCol.add(chunk0To100);
                distCol.moveRangeAtSync(range0To100, place(1), mm);
                break;
            case 1:
                distCol.add(chunk100To200);
                distCol.moveRangeAtSync(range100To200, place(2), mm);
                break;
            case 2:
                distCol.add(chunk200To250);
                distCol.moveRangeAtSync(range200To250, place(3), mm);
                break;
            default:
            }
            mm.sync();

            // Update distribution
            distCol.updateDist();

            // Debug print
            for (final LongRange lr : distCol.ranges()) {
                System.out.println(here() + ":rank" + world.rank() + " contains " + lr);
            }

            // Check contents of the lrd
            assertEquals(place(1), lrd.location(range0To100));
            assertEquals(place(2), lrd.location(range100To200));
            assertEquals(place(3), lrd.location(range200To250));
        });
    }

    @Test
    public void testDistributionSubscribed4() {
        final LongRangeDistribution distrib = distCol.getDistribution();
        distCol.registerDistribution(distrib);

        at(place(1), () -> distCol.add(chunk0To100));
        at(place(2), () -> distCol.add(chunk100To200));
        at(place(3), () -> distCol.add(chunk200To250));

        world.broadcastFlat(() -> distCol.updateDist());

        assertEquals(place(1), distrib.location(range0To100));
        assertEquals(place(2), distrib.location(range100To200));
        assertEquals(place(3), distrib.location(range200To250));

        at(place(1), () -> {
            final OneSidedMoveManager osmm = new OneSidedMoveManager(place(2));
            distCol.moveRangeAtSync(new LongRange(50, 70), place(2), osmm);
            osmm.asyncSend();
        });

        world.broadcastFlat(() -> distCol.updateDist());

        assertEquals(place(1), distrib.location(new LongRange(0, 50)));
        assertEquals(place(1), distrib.location(new LongRange(70, 100)));
        assertEquals(place(2), distrib.location(range100To200));
        assertEquals(place(2), distrib.location(new LongRange(50, 70)));
        assertEquals(place(3), distrib.location(range200To250));
    }

    /**
     * Checks that the global forEach operates as intended
     *
     * @throws Throwable if such a throwable is thrown during the test
     */
    @Test(timeout = 10000)
    public void testGlobalForEach() throws Throwable {
        try {
            // Place chunks in different handles
            distCol.placeGroup().broadcastFlat(() -> {
                switch (here().id) {
                case 0:
                    distCol.add(chunk0To100);
                    break;
                case 1:
                    distCol.add(chunk100To200);
                    break;
                case 2:
                    distCol.add(chunk200To250);
                    break;
                }
            });

            // Call GLOBAL forEach
            distCol.GLOBAL.forEach((e) -> {
                e.s = "testGlobal" + e.s;
            });

            // Check that every string was modified

            distCol.placeGroup().broadcastFlat(() -> {
                for (final Element e : distCol) {
                    assertTrue(e.s.startsWith("testGlobal"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Checks that the global parallelForEach operates as intended
     *
     * @throws Throwable if such a throwable is thrown during the test
     */
    @Test(timeout = 10000)
    public void testGlobalParallelForEach() throws Throwable {
        try {
            // Place chunks in different handles
            distCol.placeGroup().broadcastFlat(() -> {
                switch (here().id) {
                case 0:
                    distCol.add(chunk0To100);
                    break;
                case 1:
                    distCol.add(chunk100To200);
                    break;
                case 2:
                    distCol.add(chunk200To250);
                    break;
                }
            });

            // Call GLOBAL parallelForEach
            distCol.GLOBAL.parallelForEach((e) -> {
                e.s = "testGlobal" + e.s;
            });

            // Check that every string was modified

            distCol.placeGroup().broadcastFlat(() -> {
                for (final Element e : distCol) {
                    assertTrue(e.s.startsWith("testGlobal"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Tests the case where the range which is transmitted is the "left side" of an
     * existing chunk
     *
     * @throws IOException if thrown during the test
     */
    @Test
    public void testMoveRangeAtSyncLong_leftRange() throws IOException {
        distCol.add(chunk0To100);
        distCol.add(chunk100To200);
        distCol.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(0l, 50l);

        distCol.moveRangeAtSync(toTransfer, place(1), m);
        m.send();

        assertFalse(distCol.contains(toTransfer));
        at(place(1), () -> assertTrue(distCol.containsRange(toTransfer)));
    }

    /**
     * Tests the
     * {@link DistChunkedList#moveRangeAtSync(LongRange, apgas.Place, MoveManager)}
     * method in a situation where the range to move matches that of an existing
     * chunk
     *
     * @throws IOException if thrown during the test
     */
    @Test
    public void testMoveRangeAtSyncLong_matchingRange() throws IOException {
        distCol.add(chunk0To100);
        distCol.add(chunk100To200);
        distCol.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        distCol.moveRangeAtSync(range0To100, place(1), m); // Should not throw anything
        m.send();

        assertFalse(distCol.contains(chunk0To100));
        at(place(1), () -> assertTrue(distCol.containsRange(range0To100)));
    }

    /**
     * Tests the case where the range which is transmitted is the "left side" of an
     * existing chunk
     *
     * @throws IOException if thrown during the test
     */
    @Test
    public void testMoveRangeAtSyncLong_middleRange() throws IOException {
        distCol.add(chunk0To100);
        distCol.add(chunk100To200);
        distCol.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(25l, 75l);

        distCol.moveRangeAtSync(toTransfer, place(1), m);
        m.send();

        assertFalse(distCol.contains(toTransfer));
        at(place(1), () -> assertTrue(distCol.containsRange(toTransfer)));
    }

    /**
     * Tests the case where the range which is transmitted is the "left side" of an
     * existing chunk
     *
     * @throws IOException if thrown during the test
     */
    @Test
    public void testMoveRangeAtSyncLong_rightRange() throws IOException {
        distCol.add(chunk0To100);
        distCol.add(chunk100To200);
        distCol.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(50l, 100l);

        distCol.moveRangeAtSync(toTransfer, place(1), m);
        m.send();

        assertFalse(distCol.contains(toTransfer));
        at(place(1), () -> assertTrue(distCol.containsRange(toTransfer)));
    }

    /**
     * Checks that the distCol is in the expected state after method
     * {@link #setUp()} is called
     */
    @Test
    public void testSetup() {
        assertEquals(distCol, distCol.id().getHere());
        world.broadcastFlat(() -> {
            assertTrue(distCol.isEmpty());
            assertEquals(0l, distCol.size());
            assertEquals(distCol, distCol.id().getHere());
        });
    }

}
