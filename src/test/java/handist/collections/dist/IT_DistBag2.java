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
import java.util.ArrayList;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.Place;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.dist.DistBag.DistBagTeam;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test class dedicated to class {@link DistBag} and its Global, and
 * {@link DistBagTeam}
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 3, launcher = TestLauncher.class)
public class IT_DistBag2 implements Serializable {

    static class Element implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = -4067227682215616317L;
        final Place p;
        final int key;
        int val;

        public Element(Place p, int key) {
            this.p = p;
            this.key = key;
            val = 0;
        }

        public int inc() {
            return val++;
        }

        public String toSting() {
            return "X:" + p + ", key:" + key + ", val" + val;
        }

    }

    /** Serial Version UID */
    private static final long serialVersionUID = 8208565684336617060L;

    static String genKey(Place p, int i) {
        return "" + p + ":" + i;
    }

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();
    // TODO implement tests for the various features of class DistBag
    /** PlaceGroup on which the DistMap is defined on */
    TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();

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

    @SuppressWarnings("unused")
    @Test(timeout = 30000)
    public void testSharedSerializable() throws Throwable {
        final DistBag<Element> dbag0 = new DistBag<>();
        final DistBag<Element> dbag1 = new DistBag<>();
        final Place caller = here();
        pg.broadcastFlat(() -> {
            final ArrayList<Element> elems = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                elems.add(new Element(here(), i));
            }
            dbag0.addBag(elems);
            dbag0.addBag(elems);
            dbag1.addBag(elems);
            dbag1.addBag(elems);
            final CollectiveRelocator.Gather m = new CollectiveRelocator.Gather(pg, caller);
            dbag0.TEAM.gather(m);
            dbag1.TEAM.gather(m);
            m.execute();
        });
        final int[][] board = new int[4][4];
        System.out.println("Dbag0-----");
        dbag0.forEach((Element e) -> {
            // System.out.println(e);
        });
        System.out.println("Dbag0 writting-----");
        dbag0.forEach((Element e) -> {
            final int v = board[e.key][e.inc()]++;
            // System.out.println("board:" + v);
            // System.out.println(e);
        });
        System.out.println("Dbag1-----");
        dbag1.forEach((Element e) -> {
            // System.out.println(e);
        });
        System.out.println("Dbag1 writting-----");
        dbag1.forEach((Element e) -> {
            final int v = board[e.key][e.inc()]++;
            // System.out.println("board:" + v);
            // System.out.println(e);
        });

        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                assertTrue(board[i][j] == 3);
            }
        }
    }
}
