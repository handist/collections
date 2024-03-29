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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
public class IT_DistMultiMap2 implements Serializable {

    /** Random object used to generate values */
    static Random random = new Random(12345l);

    /** Serial Version UID */
    private static final long serialVersionUID = -4212799809586932921L;

    private static String genRandStr(String prefix) {
        final long rndLong = random.nextLong();
        return prefix + rndLong;
    }

    /** Instance under test */
    DistMultiMap<String, Element> distMultiMap;

    /** Maximum number of entries in each map */
    final private int MAX_NB_VALUES_PER_MAPPING = 50;

    /** Number of mappings on each host */
    final private long NB_MAPPINGS = 20l;

    final private TeamedPlaceGroup WORLD = TeamedPlaceGroup.getWorld();

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
        distMultiMap = new DistMultiMap<>(WORLD);
        WORLD.broadcastFlat(() -> {
            final int here = WORLD.rank();
            for (long l = 0; l < NB_MAPPINGS; l++) {
                final String key = genRandStr(here + "k");

                final int nbMappings = 1 + random.nextInt(MAX_NB_VALUES_PER_MAPPING - 1);
                final List<Element> values = new ArrayList<>(nbMappings);
                for (int v = 0; v < nbMappings; v++) {
                    values.add(new Element(genRandStr(here + "v" + l + "m")));
                }
                distMultiMap.put(key, values);
            }
        });
    }

    @After
    public void tearDown() throws Throwable {
        distMultiMap.destroy();
    }

    @Test(timeout = 5000)
    public void testGetObjectDispatcher() throws Throwable {
        try {
            WORLD.broadcastFlat(() -> {
                final Distribution<String> rule = ((String key) -> {
                    return WORLD.get(0);
                });
                final MultiMapEntryDispatcher<String, Element> dispatcher = distMultiMap.getObjectDispatcher(rule);
                dispatcher.put1("test", new Element("test" + WORLD.rank()));
                dispatcher.TEAM.dispatch();
                if (WORLD.rank() == 0) {
                    assertEquals(WORLD.size(), distMultiMap.get("test").size());
                } else {
                    assertFalse(distMultiMap.containsKey("test"));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 5000)
    public void testGlobalForEach() throws Throwable {
        final String prefix = "TESTGLOBALFOREACH";
        // Add a prefix to all the first element of the lists
        distMultiMap.GLOBAL.forEach((l) -> {
            final Element firstElement = l.iterator().next();
            firstElement.s = prefix + firstElement.s;
        });

        // Check the prefix was added to all first mappings of each key
        try {
            WORLD.broadcastFlat(() -> {
                for (final Collection<Element> mappings : distMultiMap.values()) {
                    // The first mapping has the prefix
                    final Iterator<Element> iter = mappings.iterator();
                    assertTrue(iter.next().s.startsWith(prefix));

                    // The remaining mappings were left untouched
                    while (iter.hasNext()) {
                        assertFalse(iter.next().s.startsWith(prefix));
                    }
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
                final int here = WORLD.rank();
                final Set<Map.Entry<String, Collection<Element>>> entrySet = distMultiMap.entrySet();
                assertEquals(entrySet.size(), NB_MAPPINGS);
                for (final Map.Entry<String, Collection<Element>> entry : entrySet) {
                    assertTrue(entry.getKey().startsWith(here + "k"));
                    for (final Element e : entry.getValue()) {
                        assertTrue(e.s.startsWith(here + "v"));
                    }
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }
}
