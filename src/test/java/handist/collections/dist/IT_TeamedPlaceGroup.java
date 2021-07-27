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

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

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
