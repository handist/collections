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
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_CachableChunkedList implements Serializable {

    static class Particle implements Serializable {
        /**
         *
         */
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

    static final int base = 100;

    static final int cbase = 10;

    // place0: [100,104)
    // place1: [200, 204) [210, 214)
    // place2: [300, 304) .. [320, 324)
    // place3: [400, 404) .. [430, 434)
    // range2 [101 303) -> shared
    // range1 [102 201) -> loop
    static LongRange subRange1 = new LongRange(2 + base, base * 2 + 1);
    static LongRange subRange2 = new LongRange(1 + base, base * 3 + 3);

    static Place getOwner(long index) {
        final int p = ((int) index) / base;
        final int off = ((int) index) % base;
        if (off % cbase < 4) {
            return new Place(p - 1);
        }
        return null;
    }

    static int getOwnerNum(Place p) {
        return (1 + p.id) * 4;
    }

    /**
     * {@link DistMap} instance under test. Before each test, it is re-initialized
     * with {@value #numData} entries placed into it on host 0 and kept empty on
     * other hosts.
     *
     * @see #setUp()
     */
    CachableChunkedList<Particle> caChunks;
    /** PlaceGroup object representing the collaboration between processes */
    TeamedPlaceGroup placeGroup;

    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        DistLog.globalSetup(placeGroup, 0, true);
        caChunks = new CachableChunkedList<>(placeGroup);
        placeGroup.broadcastFlat(() -> {
            final int n = here().id + 1;
            for (int i = 0; i < n; i++) {
                final long from = base * n + cbase * i;
                final Chunk<Particle> chunk = new Chunk<>(new LongRange(from, from + numData),
                        (Long index) -> new Particle(index));
                caChunks.add(chunk);
            }
        });
    }

    public void testForAllReduce(final CachableChunkedList<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                ca.forEach((Particle p) -> {
                    p.force = 100;
                });
                ca.forEach((Particle e) -> {
                    assertEquals(e.force, 100);
                    DistLog.log("5allReduceBefore:" + e.pos, e.toString(), null);
                });
                ca.allreduce(Collections.singletonList(subRange1), (Particle p) -> p.force, (Particle p, Long v) -> {
                    p.force += v;
                });
                ca.forEach((long index, Particle p) -> {
                    if (subRange1.contains(index)) {
                        assertEquals(p.force, 400);
                    } else {
                        assertEquals(p.force, 100);
                    }
                    DistLog.log("6AlleduceAfter:" + p.pos, p.toString(), null);
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    public void testForBcast(final CachableChunkedList<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                ca.forEachSharedOwner(subRange1, (Particle p) -> {
                    p.force = 10;
                });
                ca.forEach((long index, Particle p) -> {
                    DistLog.log("2BcastBefore:" + p.pos, p.toString(), null);
                    if (getOwner(index).equals(here()) && subRange1.contains(index)) {
                        assertEquals(p.force, 10);
                    } else {
                        assertNotEquals(p.force, 10);
                    }
                });

                ca.bcast(subRange1, (Particle p) -> p.force, (Particle p, Long v) -> {
                    p.force = v;
                });
                ca.forEach((long index, Particle e) -> {
                    if (subRange1.contains(index)) {
                        assertEquals(e.force, 10);
                    } else {
                        assertNotEquals(e.force, 10);
                    }
                    DistLog.log("2BcastDONE:" + e.pos, e.toString(), null);
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    public void testForEach(final CachableChunkedList<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                ca.forEach((long index, Particle p) -> {
                    p.force++;
                    DistLog.log("0forEach0:" + index, p.toString(), null);
                });
                ca.forEach(subRange2, (long index, Particle p) -> {
                    p.force++;
                    DistLog.log("0forEach1:" + index, p.toString(), null);
                });
                final AtomicInteger x = new AtomicInteger(0);
                ca.forEach((long index, Particle p) -> {
                    x.incrementAndGet();
                    if (subRange2.contains(index)) {
                        assertEquals(p.force, 2);
                    } else {
                        assertEquals(p.force, 1);
                    }
                });
                DistLog.log("OforEach2sum", Integer.toString(x.get()), null);
                assertEquals(x.get(), getOwnerNum(here()));
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    public void testForReduce(final CachableChunkedList<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                ca.forEach((Particle p) -> {
                    p.force = 20;
                });
                ca.forEach((Particle e) -> {
                    assertEquals(e.force, 20);
                    DistLog.log("3ReduceBefore:" + e.pos, e.toString(), null);
                });
                ca.reduce(Collections.singletonList(subRange1), (Particle p) -> p.force, (Particle p, Long v) -> {
                    p.force += v;
                });
                ca.forEach((Particle e) -> {

                    DistLog.log("4ReduceAfter:" + e.pos, e.toString(), null);
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    public void testForShare(final CachableChunkedList<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                ca.share(subRange2);
                ca.forEach((Particle e) -> {
                    DistLog.log("1Shared:" + e.pos, e.toString(), null);
                });
                ca.shared.forEachChunk((RangedList<Particle> chunk) -> {
                    assertTrue(subRange2.contains(chunk.getRange()));
                    DistLog.log("1Shared:" + chunk.getRange(), ca.getSharedOwner(chunk).toString(), null);
                });
                assertEquals(ca.shared2owner.size(), 4);
                ca.forEach((long index, Particle p) -> {
                    assertTrue(getOwner(index).equals(here()) || subRange2.contains(index));
                });
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Checks that the initialization of the distMap was done correctly
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 30000)
    public void testSimple() throws Throwable {
        testForEach(caChunks);
        testForShare(caChunks);
        testForBcast(caChunks);
        testForReduce(caChunks);
        testForAllReduce(caChunks);

        DistLog.defaultGlobalGather();
        // DistLog.defaultLog.printAll(System.out);
    }

}
