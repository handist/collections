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
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
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

    @Before
    public void setUp() throws Throwable {
        distIdMap = new DistIdMap<>();
        WORLD.broadcastFlat(() -> {
            final int here = WORLD.rank();
            for (long index = here * ENTRIES_PER_PLACE; index < (here + 1) * ENTRIES_PER_PLACE; index++) {
                distIdMap.put(index, new Element(genRandStr(here + "p")));
            }
        });
    }

    @After
    public void tearDown() throws Throwable {
        distIdMap.destroy();
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
    public void testSetUp() throws Throwable {
        try {
            WORLD.broadcastFlat(() -> {
                // Check that the correct nb of entries have benn initialized
                assertEquals(ENTRIES_PER_PLACE, distIdMap.size());

                // Check all local elements have the correct prefix
                final int here = WORLD.rank();
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
