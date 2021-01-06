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

import static apgas.Constructs.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=4, launcher=TestLauncher.class)
public class IT_TeamedPlaceGroup implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = -1060428318155098035L;

	TeamedPlaceGroup world;

	@Before
	public void setup() {
		world = TeamedPlaceGroup.getWorld();
	}

	@Test(timeout=60000)
	public void testSplitWorldInHalf() throws Throwable {
		finish(() -> {
			world.broadcastFlat(() -> {
				// System.out.println("hello:" + here() + ", " + world);
				TeamedPlaceGroup split = world.splitHalf();
				// System.out.println("split hello:" + here() + ", " + split);
				if (split.rank() == 0) {
					if (world.rank() == 0) {
						IT_DistMap test = new IT_DistMap();
						test.pg = split; //TODO this is not very clean
						test.setup();
						try {
							test.run();
						} catch (Throwable t) {
							throw new RuntimeException(t);
						}
						// System.out.println("----finishA");
					} else {
						IT_DistMultiMap test = new IT_DistMultiMap();
						test.pg = split; //TODO this is not very clean
						test.setup();
						try {
							test.run();							
						} catch (Throwable t) {
							throw new RuntimeException(t);
						}
					}
				}
			});
		});
	}
	
	@Ignore
	@Test
	public void testRank() throws Throwable {
		// I don't quite understand the relationship between rank and place number. 
		// Until then this test will be skipped.
		// Check that every place has a correct rank in world place group
		try {
			world.broadcastFlat(()->{
				assertEquals(here().id, world.rank());
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}

		TeamedPlaceGroup zeroAndOne = world.splitHalf();
		assertEquals(0, zeroAndOne.rank(here()));
		assertEquals(0, zeroAndOne.rank());
		assertEquals(0, zeroAndOne.rank(place(1)));
		assertThrows(RuntimeException.class, ()->zeroAndOne.rank(place(2)));
		assertThrows(RuntimeException.class, ()->zeroAndOne.rank(place(3)));

		try {
			asyncAt(place(2), ()->{
				zeroAndOne.rank();
			});
		} catch (MultipleException me) {
			assertEquals(RuntimeException.class, me.getSuppressed()[0].getClass());
		}
	}
}
