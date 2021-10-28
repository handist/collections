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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test class for the distributed features of {@link DistBag}
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_DistBag implements Serializable {

    /** Number of elements to initialize on each host */
    static final int NB_ELEMS[] = { 100, 50 };

    static final int NB_LISTS[] = { 4, 4 };
    static Random random = new Random(12345l);

    /** Serial Version UID */
    private static final long serialVersionUID = 7668710704105520109L;

    /** World place group */
    static final TeamedPlaceGroup WORLD = TeamedPlaceGroup.getWorld();

    public static String genRandomString(String header) {
        final long rand = random.nextLong();
        return header + rand;
    }

    /** Instance under test */
    DistBag<Element> distBag;

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

    @After
    public void cleanup() throws Throwable {
        distBag.destroy();
    }

    @Before
    public void setup() throws Throwable {
        distBag = new DistBag<>();
        WORLD.broadcastFlat(() -> {
            final int here = WORLD.rank();
            for (int listNumber = 0; listNumber < NB_LISTS[here]; listNumber++) {
                final List<Element> l = new ArrayList<>(NB_ELEMS[here]);
                for (int i = 0; i < NB_ELEMS[here]; i++) {
                    l.add(new Element(genRandomString(here + "p")));
                }
                distBag.addBag(l);
            }
        });
    }

    @Test
    public void testGlobalForEach() throws Throwable {
        // Add a prefix to all Element.s members
        distBag.GLOBAL.forEach((e) -> {
            e.s = "GLOBAL" + e.s;
        });

        // Check that all elements on all places have the new prefix
        try {
            WORLD.broadcastFlat(() -> {
                // "normal" for loop on the elements of the local handle
                for (final Element e : distBag) {
                    assertTrue(e.s.startsWith("GLOBAL"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test
    public void testGlobalParallelForEach() throws Throwable {
        // Add a prefix to all Element.s members
        distBag.GLOBAL.parallelForEach((e) -> {
            e.s = "GLOBAL" + e.s;
        });

        // Check that all elements on all places have the new prefix
        try {
            WORLD.broadcastFlat(() -> {
                // "normal" for loop on the elements of the local handle
                for (final Element e : distBag) {
                    assertTrue(e.s.startsWith("GLOBAL"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 5000)
    public void testGlobalSize() throws Throwable {
        final long[] size = new long[WORLD.size()];
        final long[] expected = new long[WORLD.size()];
        for (int i = 0; i < WORLD.size(); i++) {
            expected[i] = NB_ELEMS[i] * NB_LISTS[i];
        }

        distBag.GLOBAL.getSizeDistribution(size);

        assertArrayEquals(expected, size);
    }

    @Test
    public void testSetup() throws Throwable {
        WORLD.broadcastFlat(() -> {
            final int here = WORLD.rank();
            assertEquals(NB_LISTS[here] * NB_ELEMS[here], distBag.size());
            for (final Element e : distBag) {
                assertTrue(e.s.startsWith(here + "p"));
            }
        });
    }

    @Test(timeout = 5000)
    public void testTeamSize() throws Throwable {
        final long[] expected = new long[WORLD.size()];
        for (int i = 0; i < WORLD.size(); i++) {
            expected[i] = NB_ELEMS[i] * NB_LISTS[i];
        }

        try {
            WORLD.broadcastFlat(() -> {
                final long[] size = new long[WORLD.size()];
                distBag.TEAM.getSizeDistribution(size);
                assertArrayEquals(expected, size);
            });
        } catch (final MultipleException me) {
            System.err.println("Error occurred in testTeamSize: Suppressed errors were:");
            for (final Throwable t : me.getSuppressed()) {
                t.printStackTrace();
            }
            throw me.getSuppressed()[0];
        }
    }
}
