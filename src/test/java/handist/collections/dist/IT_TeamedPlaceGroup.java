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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.Place;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;
import mpi.MPI;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_TeamedPlaceGroup implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -1060428318155098035L;

    TeamedPlaceGroup world;

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

    @Before
    public void setup() {
        world = TeamedPlaceGroup.getWorld();
    }

    @Test
    public void testAllGather1() {
        world.broadcastFlat(() -> {
            final int value = world.rank();

            final double[] d = world.allGather1((double) value);
            final float[] f = world.allGather1((float) value);
            final int[] i = world.allGather1(value);
            final long[] l = world.allGather1((long) value);
            final short[] s = world.allGather1((short) value);

            for (int j = 0; j < world.size(); j++) {
                assertEquals(j, Math.round(d[j]));
                assertEquals(j, Math.round(f[j]));
                assertEquals(j, i[j]);
                assertEquals(j, (int) l[j]);
                assertEquals(j, s[j]);
            }
        });
    }

    @Test
    public void testAllReduce1() {
        world.broadcastFlat(() -> {
            final int value = world.rank();

            final double d = world.allReduce1((double) value, MPI.SUM);
            final float f = world.allReduce1((float) value, MPI.SUM);
            final int i = world.allReduce1(value, MPI.SUM);
            final long l = world.allReduce1((long) value, MPI.SUM);
            final short s = world.allReduce1((short) value, MPI.SUM);

            assertEquals(6, Math.round(d));
            assertEquals(6, Math.round(f));
            assertEquals(6, i);
            assertEquals(6l, l);
            assertEquals(6, s);
        });
    }

    @Test
    public void testBCast1() {
        world.broadcastFlat(() -> {
            final Place root = world.get(0);

            if (world.rank() == 0) {
                world.bCast1(101.0, root); // double
                world.bCast1(102.0f, root); // float
                world.bCast1(103, root); // int
                world.bCast1(104l, root); // long
                world.bCast1((short) 105, root);// short
            } else {
                final double d = world.bCast1(0.0, root);
                final double f = world.bCast1(0f, root);
                final int i = world.bCast1(0, root);
                final long l = world.bCast1(0l, root);
                final short s = world.bCast1((short) 0, root);
                assertEquals(101, Math.round(d));
                assertEquals(102, Math.round(f));
                assertEquals(103, i);
                assertEquals(104l, l);
                assertEquals(105, s);

            }
        });
    }

    @Test
    public void testGather1() {
        world.broadcastFlat(() -> {
            final Place root = world.get(0);
            final int value = world.rank();

            final double[] d = world.gather1((double) value, root);
            final float[] f = world.gather1((float) value, root);
            final int[] i = world.gather1(value, root);
            final long[] l = world.gather1((long) value, root);
            final short[] s = world.gather1((short) value, root);

            if (world.rank() == 0) {
                // expect { 0, 1, 2, 3 }
                for (int j = 0; j < world.size(); j++) {
                    assertEquals(j, Math.round(d[j]));
                    assertEquals(j, Math.round(f[j]));
                    assertEquals(j, i[j]);
                    assertEquals(j, (int) l[j]);
                    assertEquals(j, s[j]);
                }
            } else {
                assertNull(d);
                assertNull(f);
                assertNull(i);
                assertNull(l);
                assertNull(s);
            }
        });
    }

    @Test
    public void testReduce1() {
        world.broadcastFlat(() -> {
            final Place root = world.get(0);
            final int value = world.rank();

            final double d = world.reduce1((double) value, MPI.SUM, root);
            final float f = world.reduce1((float) value, MPI.SUM, root);
            final int i = world.reduce1(value, MPI.SUM, root);
            final long l = world.reduce1((long) value, MPI.SUM, root);
            final short s = world.reduce1((short) value, MPI.SUM, root);

            if (world.rank() == 0) {
                assertEquals(6, Math.round(d));
                assertEquals(6, Math.round(f));
                assertEquals(6, i);
                assertEquals(6l, l);
                assertEquals(6, s);
            } else {
                assertEquals(value, Math.round(d));
                assertEquals(value, Math.round(f));
                assertEquals(value, i);
                assertEquals(value, (int) l);
                assertEquals(value, s);
            }
        });
    }

    @Test(timeout = 60000)
    public void testSplitWorldInHalf() throws Throwable {
        finish(() -> {
            world.broadcastFlat(() -> {
                // System.out.println("hello:" + here() + ", " + world);
                final TeamedPlaceGroup split = world.splitHalf();
                // System.out.println("split hello:" + here() + ", " + split);
                if (split.rank() == 0) {
                    if (world.rank() == 0) {
                        final IT_DistMap test = new IT_DistMap();
                        test.pg = split; // TODO this is not very clean
                        test.setup();
                        try {
                            test.run();
                        } catch (final Throwable t) {
                            throw new RuntimeException(t);
                        }
                        // System.out.println("----finishA");
                    } else {
                        final IT_DistMultiMap test = new IT_DistMultiMap();
                        test.pg = split; // TODO this is not very clean
                        test.setup();
                        try {
                            test.run();
                        } catch (final Throwable t) {
                            throw new RuntimeException(t);
                        }
                    }
                }
            });
        });
    }
}
