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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
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
import handist.collections.function.SerializableFunction;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Tests for the distributed features of {@link DistChunkedList}
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_DistCol implements Serializable {

    /** Number of Chunks used in the test */
    static final long chunkNumber = 50;
    /** Range of each individual chunk */
    static final long rangeSize = 10;
    /** Size of the range skipped between chunks */
    static final long rangeSkip = 5;

    /** Serial Version UID */
    private static final long serialVersionUID = -9076195681727813858L;

    /** Object instance under test, initially empty */
    DistBag<List<String>> distBag;
    /** Object instance under test, initially empty */
    DistCol<String> distCol;
    /** Number of processes on which this test is running */
    int NPLACES;
    /** PlaceGroup representing the whole world */
    TeamedPlaceGroup placeGroup = TeamedPlaceGroup.getWorld();

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))) {
            System.out.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    /**
     * Prepares new instance of DistCol for the test
     */
    @Before
    public void setup() {
        placeGroup = TeamedPlaceGroup.world;
        NPLACES = placeGroup.size();
        distCol = new DistCol<>(placeGroup);
        distBag = new DistBag<>(placeGroup);
    }

    /**
     * Destroys the distributed collections that were initialized
     */
    @After
    public void tearDown() {
        distCol.destroy();
        distBag.destroy();
    }

    /**
     * Makes a number of instance transfers from place to place
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 100000)
    public void testRun() throws Throwable {
        // Prepare initial population
        long rangeBegin = 0; // inclusive
        long rangeEnd; // exclusive
        try {
            for (long i = 0; i < chunkNumber; i++) {
                rangeEnd = rangeBegin + rangeSize - 1;
                final Chunk<String> c = new Chunk<>(new LongRange(rangeBegin, rangeEnd), "<empty>");
                for (long j = rangeBegin; j < rangeEnd; j++) {
                    c.set(j, "" + j + "/" + i);
                }
                distCol.add(c);
                rangeBegin = rangeBegin + rangeSize + rangeSkip;
            }
        } catch (final Exception e) {
            System.err.println("Error on " + here());
            e.printStackTrace();
            throw e;
        }
        final long INITIAL_SIZE = distCol.size();

        // Check that the expected number of entries are indeed in DistCol
        try {
            placeGroup.broadcastFlat(() -> {
                final long expected = placeGroup.rank(here()) == 0 ? INITIAL_SIZE : 0l;
                assertEquals(expected, distCol.size());
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }

        // Distribute all entries
        z_distributeChunks();

        // Check that each place got 1 out of 2 chunk
        x_checkSize((h) -> {
            return INITIAL_SIZE / 2;
        });
        // Check that the expected shift is correct
        x_checkShift(0l);

        // Move all entries to the next place
        z_updateDist();
        z_moveToNextPlace();

        x_checkSize((h) -> {
            return INITIAL_SIZE / 2;
        });
        x_checkShift(1l);

        // Move all entries to the next-next place");
        z_updateDist();
        z_moveToNextPlace();
        z_moveToNextPlace();
        z_updateDist();

        x_checkSize((h) -> {
            return INITIAL_SIZE / 2;
        });
        x_checkShift(3l);

        // ---------------------------------------------------------------------------

        z_moveToNextPlace();

        x_checkSize((h) -> {
            return INITIAL_SIZE / 2;
        });
        x_checkShift(4l);

        z_updateDist();

        // ---------------------------------------------------------------------------
        // Move all entries to place 0
        z_moveToPlaceZero();

        // All the entries should be on place 0, size 0 elsewhere
        x_checkSize((h) -> {
            return h.id == 0 ? INITIAL_SIZE : 0l;
        });

        z_updateDist();

        // ---------------------------------------------------------------------------
        // Generate additional key/value pair
        long newEntriesCount = 0l;
        for (long i = chunkNumber; i < chunkNumber * 2; i++) {
            rangeEnd = rangeBegin + rangeSize;
            final Chunk<String> c = new Chunk<>(new LongRange(rangeBegin, rangeEnd), "<empty>");
            for (long j = rangeBegin; j < rangeEnd; j++) {
                c.set(j, "" + j + "/" + i);
            }
            newEntriesCount += c.size();
            distCol.add(c);
            rangeBegin = rangeBegin + rangeSize + rangeSkip;
        }
        final long ADDED_ENTRIES = newEntriesCount;
        x_checkSize((h) -> {
            return h.id == 0 ? INITIAL_SIZE + ADDED_ENTRIES : 0l;
        });

        // Distribute all entries with the additional keys/values
        z_distributeChunks();

        // CHECK THAT THE DISTRIBUTION IS CORRECT
        x_checkShift(0l);
        x_checkSize((h) -> {
            return (INITIAL_SIZE + ADDED_ENTRIES) / 2;
        });
        // Then remove the additional key/value
        placeGroup.broadcastFlat(() -> {
            try {
                final ArrayList<RangedList<String>> chunkList = new ArrayList<>();
                distCol.forEachChunk((RangedList<String> c) -> {
                    final LongRange r = c.getRange();
                    if (r.from / (rangeSize + rangeSkip) >= chunkNumber) {
                        chunkList.add(c);
                    }
                });
                for (final RangedList<String> chunk : chunkList) {
                    distCol.remove(chunk.getRange());
                }
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });

        z_updateDist();
        x_checkSize((h) -> {
            return INITIAL_SIZE / 2;
        });

        // ---------------------------------------------------------------------------
        // Split range into large ranges
        final long splitSizeLarge = rangeSize * (chunkNumber / 3);
        final LongRange AllRange = new LongRange(0, ((rangeSize + rangeSkip) * chunkNumber));

        placeGroup.broadcastFlat(() -> {
            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                LongRange range = new LongRange(0, splitSizeLarge);
                long dest = 0;
                while (range.from < AllRange.to) {
                    distCol.moveRangeAtSync(range, placeGroup.get((int) dest), mm);
                    range = new LongRange(range.from + splitSizeLarge, range.to + splitSizeLarge);
                    dest = (dest + 1) % NPLACES;
                }
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });
        z_updateDist();
        // TODO check

        // ---------------------------------------------------------------------------
        // Split range into smaller ranges
        final long splitSizeSmall = 4;
        placeGroup.broadcastFlat(() -> {
            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                LongRange range = new LongRange(0, splitSizeSmall);
                long dest = 0;
                while (range.from < AllRange.to) {
                    distCol.moveRangeAtSync(range, placeGroup.get((int) dest), mm);
                    range = new LongRange(range.from + splitSizeSmall, range.to + splitSizeSmall);
                    dest = (dest + 1) % NPLACES;
                }
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });

        z_updateDist();
        // TODO CHECK
    }

    private void x_checkShift(final long expectedShift) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final long shift = (expectedShift + here().id) % NPLACES;
                try {
                    // Check that each key/pair is on the right place
                    for (final LongRange lr : distCol.getAllRanges()) {
                        final long chunkNumber = lr.from / (rangeSize + rangeSkip);
                        final long apparentShift = (chunkNumber % NPLACES);
                        assertEquals(shift, apparentShift);
                    }
                } catch (final Throwable e) {
                    final RuntimeException re = new RuntimeException("Error on " + here());
                    re.initCause(e);
                    throw re;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Subroutine which checks that every place holds half of the total instances
     *
     * @param size
     * @throws Throwable if thrown during the check
     */
    private void x_checkSize(final SerializableFunction<Place, Long> size) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final long expected = size.apply(here());
                try {
                    assertEquals(expected, distCol.size());
                } catch (final Throwable e) {
                    final RuntimeException re = new RuntimeException("Error on " + here());
                    re.initCause(e);
                    throw re;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    private void z_distributeChunks() throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {

                try {
                    final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                    distCol.forEachChunk((RangedList<String> c) -> {
                        final LongRange r = c.getRange();
                        final String s = c.get(r.from);
                        // Every other chunk is sent to place 0 / 1
                        final int destination = (Integer.parseInt(s.split("/")[0])) % NPLACES;
                        // final ArrayList<RangedList<String>> cs = new ArrayList<>();
                        // cs.add(c);
                        // try {
                        distCol.moveRangeAtSync(r, placeGroup.get(destination), mm);
                        // } catch (final Exception e) {
                        // System.err.println("Error on " + here());
                        // e.printStackTrace();
                        // }
                    });
                    mm.sync();
                } catch (final Exception e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    private void z_moveToNextPlace() {
        placeGroup.broadcastFlat(() -> {

            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                final int rank = placeGroup.rank(here());
                final Place destination = placeGroup.get(rank + 1 == placeGroup.size() ? 0 : rank + 1);
                distCol.forEachChunk((c) -> {
                    // final ArrayList<RangedList<String>> cs = new ArrayList<>();
                    // cs.add(c);
                    // try {
                    distCol.moveRangeAtSync(c.getRange(), destination, mm);
                    // } catch (final Exception e) {
                    // System.err.println("Error on " + here());
                    // e.printStackTrace();
                    // throw e;
                    // }
                });
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });
    }

    private void z_moveToPlaceZero() {
        placeGroup.broadcastFlat(() -> {
            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                final Place destination = placeGroup.get(0);
                distCol.forEachChunk((c) -> {
                    // final ArrayList<RangedList<String>> cs = new ArrayList<>();
                    // cs.add(c);
                    // System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
                    // try {
                    distCol.moveRangeAtSync(c.getRange(), destination, mm);
                    // } catch (final Exception e) {
                    // System.err.println("Error on " + here());
                    // e.printStackTrace();
                    // throw e;
                    // }
                });
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });
    }

    private void z_updateDist() {
        placeGroup.broadcastFlat(() -> {
            try {
                distCol.team().updateDist();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });
    }
}
