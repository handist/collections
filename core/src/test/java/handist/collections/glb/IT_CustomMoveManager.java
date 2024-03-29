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
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.ExtendedConstructs;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.dist.DistChunkedList;
import handist.collections.dist.IT_OneSidedMoveManager;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test class for the customized class in charge of performing the transfer of
 * objects between hosts. This test inherits the ones from
 * {@link IT_OneSidedMoveManager} to ensure that this customized version has not
 * lost any functionality compared to its parent implementation.
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_CustomMoveManager extends IT_OneSidedMoveManager implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -5294814955826374667L;

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @Override
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
    public void setUp() throws Exception {
        col = new DistChunkedList<>();
        y_populateDistCol(col);
        destination = place(1);
        manager = new CustomOneSidedMoveManager(destination);
    }

    @Test(timeout = 20000)
    public void testAsyncSendAndDoNoMPI() throws Throwable {
        final CustomOneSidedMoveManager m = (CustomOneSidedMoveManager) manager;

        final AtomicLong givenAway = new AtomicLong(0l);
        col.forEachChunk(c -> {
            if ((c.getRange().from / 100) % 4 == 0) {
                col.moveRangeAtSync(c.getRange(), destination, m);
                givenAway.addAndGet(c.size());
            }
        });

        flag = false;

        finish(() -> {
            m.asyncSendAndDoNoMPI(() -> {
                asyncAt(place(0), () -> {
                    IT_OneSidedMoveManager.flag = true;
                });
            }, ExtendedConstructs.currentFinish());
        });

        assertTrue(flag);

        // Check that the distribution is correct
        final long given = givenAway.get();
        x_checkSize(p -> {
            switch (p.id) {
            case 0:
                return TOTAL_DATA_SIZE - given;
            case 1:
                return given;
            default:
                return 0l;
            }
        }, col);
    }

    @Test(timeout = 20000)
    public void testAsyncSendAndDoWithMPI() throws Throwable {
        final CustomOneSidedMoveManager m = (CustomOneSidedMoveManager) manager;

        final AtomicLong givenAway = new AtomicLong(0l);
        col.forEachChunk(c -> {
            if ((c.getRange().from / 100) % 4 == 0) {
                col.moveRangeAtSync(c.getRange(), destination, m);
                givenAway.addAndGet(c.size());
            }
        });

        flag = false;

        finish(() -> {
            m.asyncSendAndDoWithMPI(() -> {
                asyncAt(place(0), () -> {
                    IT_OneSidedMoveManager.flag = true;
                });
            }, ExtendedConstructs.currentFinish());
        });

        assertTrue(flag);

        // Check that the distribution is correct
        final long given = givenAway.get();
        x_checkSize(p -> {
            switch (p.id) {
            case 0:
                return TOTAL_DATA_SIZE - given;
            case 1:
                return given;
            default:
                return 0l;
            }
        }, col);
    }
}
