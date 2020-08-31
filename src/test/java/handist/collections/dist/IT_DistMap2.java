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
import static apgas.Constructs.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=2, launcher=TestLauncher.class)
public class IT_DistMap2 implements Serializable {

	/**
	 * Static members and constants. 
	 * These are either final or initialized in method 
	 * {@link #setUpBeforeClass()}. 
	 */
	/** Size of the dataset used for the tests **/
	public static final long numData = 200;
	/** Random object used to generate values */
	static Random random = new Random(12345l);
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
	 * {@link DistMap} instance under test.
	 * Before each test, it is re-initialized with {@value #numData} entries 
	 * placed into it on host 0 and kept empty on other hosts. 
	 * @see #setUp() 
	 */
	DistMap<String,Element> distMap;

	/** PlaceGroup object representing the collaboration between processes */
	TeamedPlaceGroup placeGroup;

	@Before
	public void setUp() throws Exception {
		placeGroup = TeamedPlaceGroup.getWorld();
		distMap = new DistMap<>(placeGroup);

		// Put some initial values in distMap
		for (long l=0; l<numData; l++) {
			distMap.put(genRandStr("k"), new Element(genRandStr("v")));
		}
	}

	@After
	public void tearDown() throws Exception {
		distMap.destroy();
	}

	/**
	 * Moves all the entries contained in host 0 to host 1
	 * @throws Exception if an exception is thrown during the test
	 */
	@Test(timeout=10000)
	public void testMoveToHost1() throws Exception {
		placeGroup.broadcastFlat(()-> {
			MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
			if (placeGroup.rank(here()) == 0) {

				Place destination = placeGroup.get(1);
				distMap.forEach((key, value)-> {distMap.moveAtSync(key, destination, mm);});
			}
			//placeGroup.barrier();
			mm.sync();

			if (placeGroup.rank(here()) == 1) {
				assertEquals(numData, distMap.size());
			} else {
				assertEquals(0l, distMap.size());			
			}});
	}

	@Test(timeout=10000)
	public void testGlobalForEach() throws Throwable {
		// Move some entries to place 1
		distMap.placeGroup().broadcastFlat(()-> {
			MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
			if (placeGroup.rank(here()) == 0) {

				Place destination = placeGroup.get(1);
				distMap.forEach((key, value)-> {
					if (value.s.endsWith("0")) {
						distMap.moveAtSync(key, destination, mm);
					}
				});
			}
			mm.sync();
		});

		// Set the every Element.s to a string starting with "new".
		distMap.global().forEach((e)-> e.s = genRandStr("new"));

		
		try {
			placeGroup.broadcastFlat(()-> {
				for (Element e : distMap.values()) {
					assertTrue(e.s.startsWith("new"));
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

	/**
	 * Checks that the initialization of the distMap was done correctly
	 */
	@Test
	public void testSetUp() {
		placeGroup.broadcastFlat(()-> {
			if (placeGroup.myrank == 0) {
				assertEquals(numData, distMap.size());
			} else {
				assertEquals(0l, distMap.size());
			}
		});
	}
}
