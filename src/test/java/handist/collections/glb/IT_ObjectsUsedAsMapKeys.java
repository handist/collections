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
import java.util.HashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.impl.DebugFinish;
import handist.collections.LongRange;
import handist.collections.dist.DistChunkedList;
import handist.collections.dist.DistributedCollection;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.glb.DistColGlbTask.DistColLambda;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * This class checks whether some objects are suitable to be used as keys into
 * maps. As part of the GLB implementation, a number of {@link HashMap}s are
 * used to contain various information either related to a certain
 * {@link DistributedCollection} or a {@link GlbOperation}. The difficulty
 * arises when objects are serialized to a place and then serialized back to
 * their original place as part of lambda expressions. This can cause problems
 * as the twice-serialized objects will be interpreted as a different key if
 * their object's {@link #equals(Object)} and {@link #hashCode()} methods are
 * not implemented correctly.
 *
 * @author Patrick Finnerty
 *
 */
@SuppressWarnings("javadoc")
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_ObjectsUsedAsMapKeys implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -6260636733862383978L;

    /** Map whose keys are instances of GlbOperation */
    @SuppressWarnings("rawtypes")
    transient static HashMap<GlbOperation, Integer> glbOperationMap;

    /** Map whose keys are instances of distributed collections */
    transient static HashMap<Object, Integer> distributedCollectionMap;

    @BeforeClass
    public static void before() throws Exception {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            System.setProperty("apgas.serialization", "true");
        });
    }

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(apgas.impl.Config.APGAS_FINISH))) {
            System.out.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    @Before
    public void setUp() throws Exception {
        glbOperationMap = new HashMap<>();
        distributedCollectionMap = new HashMap<>();
    }

    @Test
    public void testCollectionAsKey() {
        final DistChunkedList<Element> collection = new DistChunkedList<>();
        distributedCollectionMap.put(collection, new Integer(42));
        finish(() -> {
            asyncAt(place(1), () -> {
                @SuppressWarnings("rawtypes")
                final DistChunkedList c = collection;
                asyncAt(place(0), () -> {
                    assertNotNull(IT_ObjectsUsedAsMapKeys.distributedCollectionMap.get(c));
                    assertTrue(IT_ObjectsUsedAsMapKeys.distributedCollectionMap.containsKey(c));
                });
            });
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testGlbOperationAsKey() throws Exception {
        final DistChunkedList<Element> collection = new DistChunkedList<>();
        final GlbOperation<DistChunkedList<Element>, Element, LongRange, LongRange, DistChunkedList<Element>, DistColLambda<Element>> operationKey = new GlbOperation(
                collection, null, new DistFuture(collection), () -> {
                    return null;
                }, null, null);
        glbOperationMap.put(operationKey, new Integer(43));
        assertNotNull(operationKey);

        finish(() -> {
            asyncAt(place(1), () -> {
                final GlbOperation op = operationKey;
                asyncAt(place(0), () -> {
                    assertNotNull(IT_ObjectsUsedAsMapKeys.glbOperationMap.get(op));
                    assertTrue(IT_ObjectsUsedAsMapKeys.glbOperationMap.containsKey(op));
                });
            });
        });
    }

}
