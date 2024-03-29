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

import static handist.collections.glb.GlobalLoadBalancer.*;
import static handist.collections.glb.Util.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import handist.collections.dist.DistMap;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@Ignore
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_GLB_DistMap implements Serializable {

    /** Size of the data collection to test */
    static final long DATA_SIZE = 4000l;

    /** Serial Version UID */
    private static final long serialVersionUID = 2120690551298000066L;

    DistMap<String, Element> map;

    /** PlaceGroup on which collection #map is defined */
    TeamedPlaceGroup placeGroup;

    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        map = new DistMap<>(placeGroup);

        // Put some initial values in distMap
        for (long l = 0; l < DATA_SIZE; l++) {
            map.put(genRandStr("k"), new Element(genRandStr("v")));
        }
    }

    @After
    public void tearDown() throws Exception {
        map.destroy();
    }

    @Test
    public void testForEach() throws Throwable {
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                map.GLB.forEach(makePrefixTest);
            });
            if (!ex.isEmpty()) {
                throw ex.get(0);
            }

        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }

        // Check that the instances held on each place have the correct prefix
        try {
            placeGroup.broadcastFlat(() -> {
                for (final Element e : map.values()) {
                    assertTrue(e.s.startsWith("Test"));
                }
            });
        } catch (final MultipleException me) {
            for (final Throwable e : me.getSuppressed()) {
                e.printStackTrace();
            }
            throw me.getSuppressed()[0];
        }
    }

}
