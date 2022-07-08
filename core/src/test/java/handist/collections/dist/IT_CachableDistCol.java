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
import java.util.Collection;

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
import handist.collections.ElementOverlapException;
import handist.collections.LongRange;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_CachableDistCol implements Serializable {

    static class Particle implements Serializable {
        private static final long serialVersionUID = 7821371179787362791L;
        long pos;
        long force;

        Particle(long index) {
            pos = index;
            force = 0;
        }

        @Override
        public String toString() {
            return "Particle[pos: " + pos + ", force:" + force + "]";
        }
    }

    /** Size of the sata-set used for the tests **/
    public static final int numData = 4;

    /** Serial Version UID */
    private static final long serialVersionUID = 1L;

    private static final int base = 100;

    private static final int cbase = 10;

    // owner ranges
    // place0: [100,104)
    // place1: [200, 204) [210, 214)
    // place2: [300, 304) .. [320, 324)
    // place3: [400, 404) .. [430, 434)

    private static final LongRange allRange = new LongRange(100, 434);

    // range1 [101 203) -> shared (p0, p1)
    // range2 [211 303) -> shared (p0, p1, p2)
    // range3 [321 403) -> shared (p0, p1, p2, p3)
    private static final LongRange subRange1 = new LongRange(base * 1 + cbase * 0 + 1, base * 2 + 3);
    private static final LongRange subRange2 = new LongRange(base * 2 + cbase * 1 + 1, base * 3 + 3);
    private static final LongRange subRange3 = new LongRange(base * 3 + cbase * 2 + 1, base * 4 + 3);

    private static Place getOwner(long index) {
        final int p = ((int) index) / base;
        final int off = ((int) index) % base;
        if (off % cbase < 4) {
            return new Place(p - 1);
        }
        return null;
    }

    private static Collection<LongRange> getOwnerRanges(Place place) {
        final ArrayList<LongRange> ranges = new ArrayList<>();
        final int n = place.id + 1;
        for (int i = 0; i < n; i++) {
            final long from = base * n + cbase * i;
            ranges.add(new LongRange(from, from + numData));
        }
        return ranges;
    }

    /**
     * {@link DistMap} instance under test. Before each test, it is re-initialized
     * with {@value #numData} entries placed into it on host 0 and kept empty on
     * other hosts.
     *
     * @see #setUp()
     */
    private CachableDistCol<Particle> caChunks;
    /** PlaceGroup object representing the collaboration between processes */
    private TeamedPlaceGroup placeGroup;

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

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
        DistLog.globalSetup(placeGroup, 0, true);
        caChunks = new CachableDistCol<>(placeGroup);
        placeGroup.broadcastFlat(() -> {
            getOwnerRanges(here()).forEach((LongRange range) -> {
                final Chunk<Particle> chunk = new Chunk<>(range, (Long index) -> new Particle(index));
                caChunks.add(chunk);
            });
        });
    }

    @Test(timeout = 10000)
    public void testAllReduce() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {
                caChunks.forEach((Particle p) -> {
                    p.force += 100;
                });
                caChunks.allreduce((p) -> {
                    return p.force;
                }, (p, recv) -> {
                    p.force += recv;
                });

                caChunks.forEach((Particle p) -> {
                    if (subRange1.contains(p.pos)) {
                        assertEquals(200l, p.force);
                    } else if (subRange2.contains(p.pos)) {
                        assertEquals(300l, p.force);
                    } else if (subRange3.contains(p.pos)) {
                        assertEquals(400l, p.force);
                    } else {
                        assertEquals(100l, p.force);
                    }
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    // @Ignore
    // @Test(timeout = 10000)
    // TODO
    public void testAllReduceWithPrimitiveStream() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {

            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testBCast() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {
                caChunks.forEachOwn(allRange, (long index, Particle p) -> {
                    p.force += 100;
                });
                caChunks.bcast((p) -> {
                    return p.force;
                }, (p, recv) -> {
                    p.force += recv + 20;
                });

                caChunks.forEachOwn(allRange, (long index, Particle p) -> {
                    assertEquals(100l, p.force);
                });
                caChunks.forEachReceived(allRange, (long index, Particle p) -> {
                    assertEquals(120l, p.force);
                });

            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testForEachOwns() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {
                caChunks.forEachOwn(allRange, (long index, Particle p) -> {
                    assertEquals(here(), getOwner(p.pos));
                    p.force += 100;
                });
                caChunks.forEach((long index, Particle p) -> {
                    if (getOwner(index).equals(here())) {
                        assertEquals(100l, p.force);
                    } else {
                        assertEquals(0l, p.force);
                    }
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testForEachReceived() throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                caChunks.forEachReceived(allRange, (long index, Particle p) -> {
                    assertNotEquals(here(), getOwner(p.pos));
                    p.force += 100;
                });

                caChunks.forEach((long index, Particle p) -> {
                    if (getOwner(index).equals(here())) {
                        assertEquals(0l, p.force);
                    } else {
                        assertEquals(100l, p.force);
                    }
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testForEachSent() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {
                caChunks.forEachSent(allRange, (long index, Particle p) -> {
                    p.force += 100;
                });

                caChunks.forEach((long index, Particle p) -> {
                    if (caChunks.allSentRanges.containsIndex(index)) {
                        assertEquals(100l, p.force);
                    } else {
                        assertEquals(0l, p.force);
                    }
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    public void testForShare(CachableDistCol<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                switch (here().id) {
                case 0:	
                    ca.shareRangeAtSync(subRange1, new Place(1), mm);
                    break;
                case 1:	
                    ca.shareRangeAtSync(subRange1, new Place(0), mm);
                    ca.shareRangeAtSync(subRange2, new Place(0), mm);
                    ca.shareRangeAtSync(subRange2, new Place(2), mm);
                    break;
                case 2:	
                    ca.shareRangeAtSync(subRange2, new Place(0), mm);
                    ca.shareRangeAtSync(subRange2, new Place(1), mm);
                    ca.shareRangeAtSync(subRange3, new Place(0), mm);
                    ca.shareRangeAtSync(subRange3, new Place(1), mm);
                    ca.shareRangeAtSync(subRange3, new Place(3), mm);
                    break;
                case 3:	
                    ca.shareRangeAtSync(subRange3, new Place(0), mm);
                    ca.shareRangeAtSync(subRange3, new Place(1), mm);
                    ca.shareRangeAtSync(subRange3, new Place(2) ,mm);
                    break;
		}
                mm.sync();
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testRemove_Shared() throws Throwable {
        testForShare(caChunks);
        placeGroup.broadcastFlat(() -> {
            try {
                caChunks.remove(subRange3); // Should throw exeption at here
                throw new AssertionError();
            } catch (final IllegalArgumentException e) {
            }
        });
    }

    @Test(timeout = 10000)
    public void testRemove_UnShared() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {
                if (here().id == 3) {
                    caChunks.remove(new LongRange(430, 434));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testShare() throws Throwable {
        testForShare(caChunks);
        try {
            // chech if testForShare work correctly.
            placeGroup.broadcastFlat(() -> {
                // subRange1 should shared to place0, place1
                if (here().equals(new Place(0)) || here().equals(new Place(1))) {
                    assertTrue(caChunks.subList(subRange1).numChunks() > 0);
                } else {
                    assertTrue(caChunks.subList(subRange1).numChunks() == 0);
                }
                // subRange2 should shared to place0, place1, place2
                if (here().equals(new Place(0)) || here().equals(new Place(1)) || here().equals(new Place(2))) {
                    assertTrue(caChunks.subList(subRange2).numChunks() > 0);
                } else {
                    assertTrue(caChunks.subList(subRange2).numChunks() == 0);
                }
                // subRange3 should shared to all places
                assertTrue(caChunks.subList(subRange3).numChunks() > 0);
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 10000)
    public void testShare_ForException() throws Throwable {
        testForShare(caChunks);
        placeGroup.broadcastFlat(() -> {
           try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                final Place dest = new Place((here().id + 3) % placeGroup.size());
                caChunks.shareRangeAtSync(allRange, dest, mm);
                mm.sync(); // should throw exeption at here due to receive an overlapping chunk.
                throw new AssertionError();
            } catch (final ElementOverlapException e) {
            }
        });
    }

    @Test(timeout = 10000)
    public void testUpdateShared() throws Throwable {
        testForShare(caChunks);
        try {
            placeGroup.broadcastFlat(() -> {
                final LongRange range = new LongRange(101, 104);
                // share range to place2 from place1
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                if (here().equals(new Place(1))) {
                    caChunks.shareRangeAtSync(range, new Place(2), mm);
                }
                mm.sync();
                // check owner information before updateShared().
                switch (here().id) {
                case 0:
                    assertTrue(caChunks.findSentTo(new Place(1)).containsRange(range));
                    assertFalse(caChunks.findSentTo(new Place(2)).containsRange(range)); // not update yet.
                    break;
                case 1:
                    assertTrue(caChunks.findReceivedFrom(new Place(0)).containsRange(range));
                    break;
                case 2:
                    assertTrue(caChunks.findReceivedFrom(new Place(0)).containsRange(range));
                    break;
                }
                // check owner information after updateShared()
                caChunks.updateShared();
                switch (here().id) {
                case 0:
                    assertTrue(caChunks.findSentTo(new Place(1)).containsRange(range));
                    assertTrue(caChunks.findSentTo(new Place(2)).containsRange(range));
                    break;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

}
