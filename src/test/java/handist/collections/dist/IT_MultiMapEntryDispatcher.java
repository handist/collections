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
import mpjbuf.IllegalArgumentException;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_MultiMapEntryDispatcher implements Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 3171165402246707285L;

    /** Number of places this test is running on */
    static int NPLACES;

    /** Number of initial data entries places into the map */
    final static long numData = 200;

    /** Random instance used to populate the map with initial data */
    final static Random random = new Random(12345l);

    /**
     * Helper method used to generate strings with the specified prefix
     *
     * @param prefix prefix of the returned string
     * @return a randomly generated string which starts with the specified string
     */
    public static String genRandStr(String prefix) {
        final long rand = random.nextLong();
        return prefix + rand;
    }

    private DistConcurrentMultiMap<String, String> distMultiMap;

    /** PlaceGroup on which the DistMultiMap is defined on */
    TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();

    final Distribution<String> gatherDist = (key) -> {
        return pg.get(0);
    };

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))) {
            System.err.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    /**
     * Add num data to RelocationMultiMap by async. Since the code nest became so
     * deeply, divided into method.
     */
    private void asyncPut(MultiMapEntryDispatcher<String, String> map, long num) {
        async(() -> {
            for (int j = 0; j < num; j++) {
                map.put1(genRandStr("kr") + pg.rank(), genRandStr("vr") + pg.rank());
            }
        });
    }

    @Before
    public void setup() {
        NPLACES = pg.size();
        distMultiMap = new DistConcurrentMultiMap<>(pg);
    }

    @After
    public void tearDown() {
        distMultiMap.destroy();
    }

    @Test
    public void testPutWithMultiThread() throws IllegalArgumentException {
        final int numThreads = 4; // numThreads should not make numData an indivisible number.
        try {
            // add data to distMap at each place
            pg.broadcastFlat(() -> {
                for (int i = 0; i < numData; i++) {
                    distMultiMap.put1(genRandStr("k" + pg.rank()), genRandStr("v" + pg.rank()));
                }
            });
            // create RelocationMap from distMap
            final MultiMapEntryDispatcher<String, String> dispatcher = distMultiMap.getObjectDispatcher(gatherDist);
            // add data at each place and relocate
            pg.broadcastFlat(() -> {
                finish(() -> {
                    for (int i_th = 0; i_th < numThreads; i_th++) {
                        asyncPut(dispatcher, numData / numThreads);
                    }
                });
                dispatcher.TEAM.dispatch();
                // check size
                if (pg.rank() == 0) {
                    assertEquals(numData * (pg.size() + 1), distMultiMap.size());
                } else {
                    assertEquals(numData, distMultiMap.size());
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
        }
    }

    @Test
    public void testTeamRelocate() {
        try {
            // add data to distMap at each place
            pg.broadcastFlat(() -> {
                for (int i = 0; i < numData; i++) {
                    distMultiMap.put1(genRandStr("k" + pg.rank()), genRandStr("v" + pg.rank()));
                }
                // create RelocationMap from distMap
                final MultiMapEntryDispatcher<String, String> dispatcher = distMultiMap.getObjectDispatcher(gatherDist);
                // add data at each place and relocate
                for (int i = 0; i < numData; i++) {
                    dispatcher.put1(genRandStr("kr") + pg.rank(), genRandStr("vr") + pg.rank());
                }
                dispatcher.TEAM.dispatch();
                // check size
                if (pg.rank() == 0) {
                    assertEquals(numData * (pg.size() + 1), distMultiMap.size());
                } else {
                    assertEquals(numData, distMultiMap.size());
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
        }
    }

}
