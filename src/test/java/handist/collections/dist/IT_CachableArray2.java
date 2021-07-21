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
import java.util.function.BiConsumer;
import java.util.function.Function;

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
public class IT_CachableArray2 implements Serializable {

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
    }

    /**
     * Static members and constants. These are either final or initialized in method
     * {@link #setUpBeforeClass()}.
     */
    /** Size of the sata-set used for the tests **/
    public static final int numData = 4;

    /** Serial Version UID */
    private static final long serialVersionUID = 1L;
    /**
     * {@link DistMap} instance under test. Before each test, it is re-initialized
     * with {@value #numData} entries placed into it on host 0 and kept empty on
     * other hosts.
     *
     * @see #setUp()
     */
    CachableArray<Particle> carray;

    /** PlaceGroup object representing the collaboration between processes */
    TeamedPlaceGroup placeGroup;

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    public void addForceAtWorkers(final CachableArray<Particle> ca, int turn) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                for (int i = 0; i < numData; i++) {
                    final Particle p = ca.data.get(i);
                    p.force += i * placeGroup.myrank * turn;
                }
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))) {
            System.out.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    public void allreduceForce(final CachableArray<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final Function<Particle, Long> pack = (Particle elem) -> {
                    return elem.force;
                };
                final BiConsumer<Particle, Long> unpack = (Particle elem, Long val) -> {
                    elem.force += val;
                };
                ca.allreduce(pack, unpack);
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }

    }

    public void bcastPos(final CachableArray<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final Function<Particle, Long> pack = (Particle elem) -> elem.pos;
                final BiConsumer<Particle, Long> unpack = (Particle elem, Long val) -> {
                    elem.pos = val;
                };
                ca.broadcast(pack, unpack);
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }

    }

    public void checkForce(final CachableArray<Particle> ca, long val) throws Throwable {
        try {
            for (int i = 0; i < numData; i++) {
                final Particle p = ca.data.get(i);
                // System.out.println("check force["+i+"]@"+Constructs.here()+":"+p.force);
                assertEquals(p.force, i * val);
            }
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    public void checkForceAll(final CachableArray<Particle> ca, final long val) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                for (int i = 0; i < numData; i++) {
                    final Particle p = ca.data.get(i);
                    // System.out.println("check force["+i+"]@"+Constructs.here()+":"+p.force);
                    assertEquals(p.force, i * val);
                }
            });

        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    public void checkLast(final CachableArray<Particle> ca) throws Throwable {
        try {

            final long[] forces = new long[numData];
            final long[] poses = new long[numData];
            for (int i = 0; i < numData; i++) {
                final Particle p = ca.data.get(i);
                forces[i] = p.force;
                poses[i] = p.pos;
            }
            placeGroup.broadcastFlat(() -> {
                final List<Particle> local = ca.data;
                for (int j = 0; j < local.size(); j++) {
                    final Particle p2 = local.get(j);
                    // System.out.println("check pos["+j+"]@"+Constructs.here()+":"+p2.pos);
                    // System.out.println("check Rforce["+j+"]@"+Constructs.here()+":"+p2.force +
                    // ":"+ forces[j]);
                    assertEquals(p2.force, forces[j]);
                    assertEquals(p2.pos, poses[j]);
                }
            });

        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    public void reduceForce(final CachableArray<Particle> ca) throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final Function<Particle, Long> pack = (Particle elem) -> {
                    final long result = elem.force;
                    elem.force = 0;
                    return result;
                };
                final BiConsumer<Particle, Long> unpack = (Particle elem, Long val) -> {
                    elem.force += val;
                };
                ca.reduce(pack, unpack);
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }

    }

    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        final List<Particle> data = new ArrayList<>();
        for (long l = 0; l < numData; l++) {
            data.add(new Particle(l));
        }
        carray = CachableArray.make(placeGroup, data);
    }

    /**
     * Checks that the initialization of the distMap was done correctly
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 30000)
    public void testSimple() throws Throwable {
        updateLocalPos(carray);
        bcastPos(carray);
        checkLast(carray);
        addForceAtWorkers(carray, 1);
        reduceForce(carray);
        checkForce(carray, 6);

        updateLocalPos(carray);
        bcastPos(carray);
        checkLast(carray);
        addForceAtWorkers(carray, 3);
        allreduceForce(carray);
        checkForceAll(carray, 18);
    }

    public void updateLocalPos(final CachableArray<Particle> ca) throws Throwable {
        try {
            for (int i = 0; i < numData; i++) {
                final Particle p = ca.data.get(i);
                p.pos += 1000;
                p.force = 0;
            }
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }
}
