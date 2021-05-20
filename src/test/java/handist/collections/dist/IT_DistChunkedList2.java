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
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
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
public class IT_DistChunkedList2 implements Serializable {

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

    /** First chunk contained by the DistCol */
    Chunk<Element> chunk0To100;
    /** Second chunk contained by the DistCol */
    Chunk<Element> chunk100To200;
    /** Third chunk contained by the DistCol */
    Chunk<Element> chunk200To250;

    /** Instance used under test */
    DistChunkedList<Element> distChunkedList;

    /** First range on which DistCol is defined */
    LongRange range0To100;
    /** Second range on which DistCol is defined */
    LongRange range100To200;
    /** Third range on which DistCol is defined */
    LongRange range200To250;

    /** TeamedPlaceGroup representing the whole world */
    TeamedPlaceGroup world;

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

        distChunkedList = new DistChunkedList<>(world);
    }

    /**
     * Destroys the DistCol instance used for the tests
     *
     * @throws Exception is thrown during destruction
     */
    @After
    public void tearDown() throws Exception {
        distChunkedList.destroy();
    }

    /**
     * Checks that adding a chunk in a place makes remote handles aware of the
     * change
     *
     * @throws Throwable if such a throwable is thrown during the test
     */
    @Test(timeout = 10000)
    public void testAddUpdatesDistributedInformation() throws Throwable {
        distChunkedList.add(chunk0To100); // Add a chunk to local handle of place 0
        distChunkedList.placeGroup().broadcastFlat(() -> {
            final long[] size = new long[world.size()];

            distChunkedList.TEAM.updateDist(); // Here is the important call
            distChunkedList.TEAM.getSizeDistribution(size); // We check the result of TEAM.size

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
            distChunkedList.placeGroup().broadcastFlat(() -> {
                switch (here().id) {
                case 0:
                    distChunkedList.add(chunk0To100);
                    break;
                case 1:
                    distChunkedList.add(chunk100To200);
                    break;
                case 2:
                    distChunkedList.add(chunk200To250);
                    break;
                }
            });

            // Make a manual forEach
            // distCol.GLOBAL.forEach((s)->{
            // System.out.println(s); // = "testGlobal" + s;
            // });

            distChunkedList.placeGroup().broadcastFlat(() -> {
                distChunkedList.forEach((e) -> {
                    e.s = "testGlobal" + e.s;
                });
            });

            // Check that every string was modified

            distChunkedList.placeGroup().broadcastFlat(() -> {
                for (final Element e : distChunkedList) {
                    assertTrue(e.s.startsWith("testGlobal"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
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
            distChunkedList.placeGroup().broadcastFlat(() -> {
                switch (here().id) {
                case 0:
                    distChunkedList.add(chunk0To100);
                    break;
                case 1:
                    distChunkedList.add(chunk100To200);
                    break;
                case 2:
                    distChunkedList.add(chunk200To250);
                    break;
                }
            });

            // Call GLOBAL forEach
            distChunkedList.GLOBAL.forEach((e) -> {
                e.s = "testGlobal" + e.s;
            });

            // Check that every string was modified

            distChunkedList.placeGroup().broadcastFlat(() -> {
                for (final Element e : distChunkedList) {
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
            distChunkedList.placeGroup().broadcastFlat(() -> {
                switch (here().id) {
                case 0:
                    distChunkedList.add(chunk0To100);
                    break;
                case 1:
                    distChunkedList.add(chunk100To200);
                    break;
                case 2:
                    distChunkedList.add(chunk200To250);
                    break;
                }
            });

            // Call GLOBAL parallelForEach
            distChunkedList.GLOBAL.parallelForEach((e) -> {
                e.s = "testGlobal" + e.s;
            });

            // Check that every string was modified

            distChunkedList.placeGroup().broadcastFlat(() -> {
                for (final Element e : distChunkedList) {
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
        distChunkedList.add(chunk0To100);
        distChunkedList.add(chunk100To200);
        distChunkedList.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(0l, 50l);

        distChunkedList.moveRangeAtSync(toTransfer, place(1), m);
        m.send();

        assertFalse(distChunkedList.containsRange(toTransfer));
        at(place(1), () -> assertTrue(distChunkedList.containsRange(toTransfer)));
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
        distChunkedList.add(chunk0To100);
        distChunkedList.add(chunk100To200);
        distChunkedList.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        distChunkedList.moveRangeAtSync(range0To100, place(1), m); // Should not throw anything
        m.send();

        assertFalse(distChunkedList.containsRange(range0To100));
        at(place(1), () -> assertTrue(distChunkedList.containsRange(range0To100)));
    }

    /**
     * Tests the case where the range which is transmitted is the "left side" of an
     * existing chunk
     *
     * @throws IOException if thrown during the test
     */
    @Test
    public void testMoveRangeAtSyncLong_middleRange() throws IOException {
        distChunkedList.add(chunk0To100);
        distChunkedList.add(chunk100To200);
        distChunkedList.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(25l, 75l);

        distChunkedList.moveRangeAtSync(toTransfer, place(1), m);
        m.send();

        assertFalse(distChunkedList.containsRange(toTransfer));
        at(place(1), () -> assertTrue(distChunkedList.containsRange(toTransfer)));
    }

    /**
     * Tests the case where the range which is transmitted is the "left side" of an
     * existing chunk
     *
     * @throws IOException if thrown during the test
     */
    @Test
    public void testMoveRangeAtSyncLong_rightRange() throws IOException {
        distChunkedList.add(chunk0To100);
        distChunkedList.add(chunk100To200);
        distChunkedList.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(50l, 100l);
        final LongRange leftover = new LongRange(0, 50l);

        distChunkedList.moveRangeAtSync(toTransfer, place(1), m);

        assertTrue(distChunkedList.containsRange(toTransfer));
        assertTrue(distChunkedList.containsRange(range0To100));
        assertTrue(distChunkedList.containsRange(leftover));

        m.send();

        assertFalse(distChunkedList.containsRange(toTransfer));
        assertFalse(distChunkedList.containsRange(range0To100));
        assertTrue(distChunkedList.containsRange(leftover));
        at(place(1), () -> assertTrue(distChunkedList.containsRange(toTransfer)));
    }

    @Test(timeout = 1000)
    public void testMoverRangeAtSync_overlappingRange() throws IOException {
        distChunkedList.add(chunk0To100);
        distChunkedList.add(chunk100To200);
        distChunkedList.add(chunk200To250);

        final OneSidedMoveManager m = new OneSidedMoveManager(place(1));
        final LongRange toTransfer = new LongRange(50l, 150l);
        final LongRange leftover = new LongRange(0, 50l);
        final LongRange rightover = new LongRange(150l, 200l);

        distChunkedList.moveRangeAtSync(toTransfer, place(1), m);

        assertTrue(distChunkedList.containsRange(toTransfer));
        assertTrue(distChunkedList.containsRange(range0To100));
        assertTrue(distChunkedList.containsRange(range100To200));
        assertTrue(distChunkedList.containsRange(leftover));
        assertTrue(distChunkedList.containsRange(rightover));

        m.send();

        assertFalse(distChunkedList.containsRange(toTransfer));
        assertFalse(distChunkedList.containsRange(range0To100));
        assertTrue(distChunkedList.containsRange(leftover));
        assertTrue(distChunkedList.containsRange(rightover));
        at(place(1), () -> assertTrue(distChunkedList.containsRange(toTransfer)));

    }

    /**
     * Checks that the distCol is in the expected state after method
     * {@link #setUp()} is called
     */
    @Test
    public void testSetup() {
        assertEquals(distChunkedList, distChunkedList.id().getHere());
        world.broadcastFlat(() -> {
            assertTrue(distChunkedList.isEmpty());
            assertEquals(0l, distChunkedList.size());
            assertEquals(distChunkedList, distChunkedList.id().getHere());
        });
    }

}
