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
import static org.junit.Assert.*;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.dist.DistChunkedList;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.glb.lifeline.Hypercube;
import handist.collections.glb.lifeline.Loop;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test program which uses a larger dataset to be able to test load balancing
 * features.
 *
 * This test purposely initializes all the instances on which computation is
 * necessary on place 0 to force a large number of work-balance operations to be
 * made.
 *
 * @author Patrick Finnerty
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_LifelineHypercubeGLB implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 5734175886780421274L;

    /**
     * Place group that contains all the hosts in the computation
     */
    static TeamedPlaceGroup WORLD;

    /**
     * Number of chunks present in {@link #col}
     */
    public static final int CHUNK_COUNT = 10000;

    /**
     * Size of each individual chunk in {@link #col}
     */
    public static final int CHUNK_SIZE = 500;

    /**
     * Total number of elements to expect in the collection
     */
    public static final long TOTAL_SIZE = CHUNK_COUNT * CHUNK_SIZE;

    /**
     * Checks that the ranges in which the {@link DistChunkedList} is
     *
     * @param col distribution to check
     */
    private static void checkDistribution(DistChunkedList<?> col) {
        final TeamedPlaceGroup pg = col.placeGroup();
        final List<LongRange> ranges = new ArrayList<>();

        for (final Place p : pg.places()) {
            ranges.addAll(at(p, () -> {
                final ArrayList<LongRange> localRanges = new ArrayList<>();
                col.forEachChunk(c -> {
                    final LongRange range = c.getRange();
                    localRanges.add(range);
                });
                return localRanges;
            }));
        }

        ranges.sort(null); // Sort the ranges to ease the check

        final Iterator<LongRange> it = ranges.iterator();
        LongRange leftSide = it.next();
        long totalSize = leftSide.size();

        // Check that all the chunks are contiguous
        assertEquals(0l, leftSide.from);
        while (it.hasNext()) {
            final LongRange next = it.next();
            totalSize += next.size();
            assertEquals(leftSide.to, next.from);
            leftSide = next;
        }
        assertEquals(TOTAL_SIZE, leftSide.to);
        assertEquals(TOTAL_SIZE, totalSize);

        System.err.println("There was an inconsistency between the accumulated #size"
                + " and the manually checked Chunks present in the distributed collection.");
        System.err.println("It is possible a lifeline answer was still being made as the size was being computed");
    }

    /**
     * In this setup method, some properties are set to influence the behavior of
     * the GLB
     *
     * @throws Exception if thrown during setup
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            WORLD = TeamedPlaceGroup.getWorld();
            // System.setProperty(Config.ACTIVATE_TRACE, "true");
            System.setProperty(Config.GRANULARITY, "20"); // Exceptionally low
            System.setProperty(Config.LIFELINE_STRATEGY, Loop.class.getCanonicalName());
            System.setProperty(Config.MAXIMUM_WORKER_COUNT, "2"); // Set low
            System.setProperty(Config.LIFELINE_STRATEGY, Hypercube.class.getCanonicalName());
        });
    }

    /**
     * Checks that the distCol contains exactly the specified number of entries. The
     * {@link DistChunkedList#size()} needs to match the specified parameter.
     *
     * @param col           DistCol whose global size is to be checked
     * @param expectedCount expected total number of entries in the DistCol instance
     * @throws Throwable if thrown during the check
     */
    private static void z_checkDistColTotalElements(DistChunkedList<Integer> col, long expectedCount) throws Throwable {
        long count = 0;
        for (final Place p : col.placeGroup().places()) {
            count += at(p, () -> {
                return col.size();
            });
        }
        if (expectedCount != count) {
            checkDistribution(col);
        }
    }

    /** Distributed collection on which the tests are operating */
    public DistChunkedList<Integer> col;

    @Before
    public void setUp() throws Exception {
        col = new DistChunkedList<>(WORLD);
        z_populateCollection(col);
    }

    @After
    public void tearDown() throws Exception {
        col.destroy();
    }

    @Test(timeout = 30000)
    public void testHypercubeLifeline() throws Throwable {
        final ArrayList<Exception> exceptions = GlobalLoadBalancer.underGLB(() -> {
            final List<Throwable> errors = col.GLB.forEach(i -> {
                // This forEach contains some dummy operation to simulate a certain load
                final byte[] hashArray = new byte[20];
                final Random r = new Random(i);
                r.nextBytes(hashArray);
                MessageDigest digest = null;
                try {
                    digest = MessageDigest.getInstance("SHA-1");
                } catch (final NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                digest.digest(hashArray);
            }).getErrors();

            if (!errors.isEmpty()) {
                System.err.println("There were " + errors.size() + " errors in testHypercubeLifeline");
                throw new RuntimeException(errors.get(0)); // Pack the first error as the cause of the RuntimeException
            }
        });

        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }

        z_checkDistColTotalElements(col, TOTAL_SIZE);
    }

    /**
     * Helper method which populates the provided distributed collection
     *
     * @param collection collection to populate
     */
    private void z_populateCollection(DistChunkedList<Integer> collection) {
        long rangeBegin = 0; // inclusive
        long rangeEnd; // exclusive
        for (long i = 0; i < CHUNK_COUNT; i++) {
            rangeEnd = rangeBegin + CHUNK_SIZE;
            final Chunk<Integer> c = new Chunk<>(new LongRange(rangeBegin, rangeEnd), 0);
            for (long j = rangeBegin; j < rangeEnd; j++) {
                c.set(j, (int) j);
            }
            collection.add(c);
            rangeBegin = rangeBegin + CHUNK_SIZE; // Continuous Chunks
        }
    }
}
