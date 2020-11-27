/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=2, launcher=TestLauncher.class)
public class IT_CachableArray implements Serializable {

    /**
     * Static members and constants. 
     * These are either final or initialized in method 
     * {@link #setUpBeforeClass()}. 
     */
    /** Size of the sata-set used for the tests **/
    public static final long numData = 200;
    /** Random object used to generate values */
    static Random random;
    /** Serial Version UID */
    private static final long serialVersionUID = 1L;
    /**
     * Helper method to generate Strings with the provided prefix.
     * <p>
     * Can only be called after {@link #setUpBeforeClass()} as the {@link Random}
     * object instance used by this method is initialized in this method. 
     * @param prefix the String prefix of the Random string generated
     * @return a random String with the provided prefix
     */
    public static String genRandStr(String prefix) {
        long rndLong = random.nextLong();
        return prefix + rndLong;
    }

    /**
     * Prepares static members
     */
    @BeforeClass
    public static void setUpBeforeClass() {
        random = new Random(12345l);
    }

    /**
     * {@link DistMap} instance under test.
     * Before each test, it is re-initialized with {@value #numData} entries 
     * placed into it on host 0 and kept empty on other hosts. 
     * @see #setUp() 
     */
    CachableArray<LinkedList<String>> carray;

    /** PlaceGroup object representing the collaboration between processes */
    TeamedPlaceGroup placeGroup;

    public void addElems(int nth, List<LinkedList<String>> ca) {
        for(LinkedList<String> elem: ca) {
            elem.add(genRandStr(""+nth));
        }
    }

    public void checkLast(final CachableArray<LinkedList<String>> ca) throws Throwable {
        int sum = 0;

        for(LinkedList<String> elem: ca) {
            sum += elem.peekLast().hashCode();
        }
        final int sumAt0 = sum;
        try {placeGroup.broadcastFlat(()->{
            int sumX = 0;
            for(LinkedList<String> elem: ca) {
                sumX += elem.peekLast().hashCode();
            }
            
            assertEquals(sumX, sumAt0);
        });} catch (MultipleException me) {
        	throw me.getSuppressed()[0];
        }
    }

    public void relocate(final CachableArray<LinkedList<String>> ca) {
        placeGroup.broadcastFlat(()->{
            Function<LinkedList<String>,String> pack = (LinkedList<String> elem)->elem.peekLast();
            BiConsumer<LinkedList<String>, String> unpack = (LinkedList<String>elem, String bag)->{ elem.addLast(bag);};
            ca.broadcast(pack, unpack);
        });
    }
        
    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        List<LinkedList<String>> data = new ArrayList<>();
        for (long l=0; l<numData; l++) {
            LinkedList<String> elem = new LinkedList<>();
            data.add(elem);
        }
        addElems(0, data);
        carray = CachableArray.make(placeGroup, data);
    }

    /**
     * Checks that the initialization of the distMap was done correctly
     */
    @Test
    public void testSimple() throws Throwable {
        checkLast(carray);
        addElems(1, carray);
        relocate(carray);
        checkLast(carray);
    }
}
