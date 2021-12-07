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

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistIdMap2 implements Serializable {

    /** Random object used to generate values */
    static Random random = new Random(12345l);

    /** Serial Version UID */
    private static final long serialVersionUID = 7279840236820619500L;

    /** World on which the test is running */
    static TeamedPlaceGroup WORLD = TeamedPlaceGroup.getWorld();

    private static String genRandStr(String prefix) {
        final long rndLong = random.nextLong();
        return prefix + rndLong;
    }

    /** Instance under test */
    DistIdMap<Element> distIdMap;

    /** Number of entries placed into the distIdMap per place */
    long ENTRIES_PER_PLACE = 100;
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
    public void setUp() throws Throwable {
        distIdMap = new DistIdMap<>();
        WORLD.broadcastFlat(() -> {
            final int here = here().id;
            final long startIndex = here * ENTRIES_PER_PLACE;
            final long stopIndex = (here + 1) * ENTRIES_PER_PLACE;
            for (long index = startIndex; index < stopIndex; index++) {
                distIdMap.put(index, new Element(genRandStr(here + "p")));
            }
        });
    }

    @After
    public void tearDown() throws Throwable {
        distIdMap.destroy();
    }

    @Test(timeout = 5000)
    public void testDistribution() throws Throwable {
        final LongDistribution dist = distIdMap.getDistribution();
        distIdMap.registerDistribution(dist);

        // Currently, only local knowledge is known in dist
        assertEquals(place(0), dist.location(0l));
        assertEquals(place(0), dist.location(0l));
        assertNull(dist.location(ENTRIES_PER_PLACE + 2));

        WORLD.broadcastFlat(() -> distIdMap.updateDist());

        // After update, remote information is also known
        assertEquals(place(0), dist.location(0l));
        assertEquals(place(0), dist.location(0l));
        assertEquals(place(1), dist.location(ENTRIES_PER_PLACE + 2));

        // Clear local contents
        distIdMap.clear();

        // Local information is up-to-date, but remote places do not know yet
        assertNull(dist.location(0l));
        assertEquals(place(0), at(place(1), () -> {
            return distIdMap.getDistribution().location(0l);
        }));

        // Make update
        WORLD.broadcastFlat(() -> distIdMap.updateDist());

        // Now, all remote hosts know that keys have been removed
        assertNull(dist.location(0l));
        assertNull(at(place(2), () -> {
            return distIdMap.getDistribution().location(0l);
        }));

    }

    @Test(timeout = 5000)
    public void testGlobalForEach() throws Throwable {
        distIdMap.GLOBAL.forEach((e) -> {
            e.s = "testGlobal" + e.s;
        });

        try {
            WORLD.broadcastFlat(() -> {
                for (final Element e : distIdMap.values()) {
                    assertTrue(e.s.startsWith("testGlobal"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 5000)
    public void testRemove() throws Throwable {
        final long keyToRemove = 0l;
        assertNotNull(distIdMap.remove(keyToRemove)); // boxed into Long
        assertNotNull(distIdMap.remove(new Long(1)));
        assertNull(distIdMap.remove(new Object())); // not boxed into Long
    }

    @Test(timeout = 5000)
    public void testSetUp() throws Throwable {
        try {
            WORLD.broadcastFlat(() -> {
                // Check that the correct nb of entries have been initialized
                assertEquals(ENTRIES_PER_PLACE, distIdMap.size());

                // Check all local elements have the correct prefix
                final int here = here().id;
                for (final Element e : distIdMap.values()) {
                    assertTrue(e.s.startsWith(here + "p"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }
}
