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

import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.function.SerializableFunction;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=2, launcher=TestLauncher.class)
public class IT_DistMap implements Serializable {

	/** Number of places this test is running on */
	static int NPLACES;

	/** Number of initial data entries places into the map */
	final static long numData = 200;

	/** Random instance used to populate the map with initial data */
	final static Random random = new Random(12345l);
	/** Serial Version UID */
	private static final long serialVersionUID = -762013040337361823L;

	/** 
	 * Helper method used to generate strings with the specified prefix
	 * @param prefix prefix of the returned string
	 * @return a randomly generated string which starts with the specified string
	 */
	public static String genRandStr(String prefix) {
		long rand = random.nextLong();
		return prefix + rand;
	}

	/** Map which undergoes the various tests */
	private DistMap<String, String> distMap;

	/** PlaceGroup on which the DistMap is defined on */
	TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();

	@Test
	public void run() throws Throwable {
		//---------------------------------------------------------------------------
		// Create initial data at Place 0
		for (int i=0; i<numData; i++) {
			distMap.put(genRandStr("k"), genRandStr("v"));
		}
		// Check that indeed there are numData entries on place 0, 0 elsewhere
		x_checkSize((j)->{return j == 0 ? numData : 0l;});

		//---------------------------------------------------------------------------
		// Distribute all entries
		z_distribute();

		// We track the number of entries received by each place in an array
		// Due to the distribution based on a hash, the distribution is not 
		// perfectly uniform
		long size [] = new long[pg.size()];
		int i = 0;
		for(Place p : pg.places()) {
			size[i++] = at(p,()->{return distMap.size();});			
		}
		
		// Check that the keys received correspond to what is expected based on the hash
		x_checkKeyShift(0);

		// ---------------------------------------------------------------------------
		// Move all entries to the next place
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				int rank = pg.rank(here());
				Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
				distMap.forEach((String key, String value) -> {
					distMap.moveAtSync(key, destination, mm);
				});

				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on "+here());
				e.printStackTrace();
				throw e;
			}
		});

		// Number of entries / place have now shifted
		x_checkSize((j)->{return size[(j + 1)%distMap.placeGroup().size()];});
		// Check that the keys have shifted by 1
		x_checkKeyShift(1);

		// ---------------------------------------------------------------------------
		// Move all entries to the next place, twice
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				int rank = pg.rank(here());
				Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);

				distMap.forEach((String key, String value) -> {
					distMap.moveAtSync(key, destination, mm);
				});
				mm.sync();
				distMap.forEach((String key, String value) -> {
					distMap.moveAtSync(key, destination, mm);
				});
				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on "+here());
				e.printStackTrace();
				throw e;
			}
		});

		// Number of entries / place have now shifted
		x_checkSize((j)->{return size[(j + 3)%distMap.placeGroup().size()];});
		// Check that the keys have shifted by 1
		x_checkKeyShift(3);


		// ---------------------------------------------------------------------------
		// Move all entries to place 0
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				Place destination = pg.get(0);
				distMap.forEach((String key, String value) -> {
					distMap.moveAtSync(key, destination, mm);
				});
				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on "+here());
				e.printStackTrace();
				throw e;
			}
		});

		// All entries should be back on Place 0
		x_checkSize((j)->{return j == 0 ? numData : 0l;});
	}

	@Before
	public void setup() {
		NPLACES = pg.size();
		distMap = new DistMap<String, String>(pg);
	}

	@After
	public void tearDown() {
		distMap.destroy();
	}

	private void x_checkKeyShift(int expectedShift) throws Throwable {
		try {
			pg.broadcastFlat(()-> {
				int PLACES = distMap.placeGroup.size();
				final int shift = (expectedShift + pg.rank()) % PLACES;
				try {
					// Check that each key/pair is on the right place
					for (String key : distMap.getAllKeys()) {
						int hash = Math.abs(key.hashCode());
						int apparentShift = (hash % PLACES);
						assertEquals(shift, apparentShift);
					}} catch(Throwable e) {
						System.err.println("Error on " + here());
						e.printStackTrace();
						throw e;
					}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

	/**
	 * Subroutine which checks that every place holds half of the total instances
	 * @param size indicates the expected size as a function of the place provided as parameter
	 * @throws Throwable if thrown during the check
	 */
	private void x_checkSize(final SerializableFunction<Integer, Long> size) throws Throwable {
		try {
			pg.broadcastFlat(()-> {
				long expected = size.apply(pg.rank());
				try {
					assertEquals(expected, distMap.size());	
				} catch(Throwable e) {
					System.err.println("Error on " + here());
					e.printStackTrace();
					throw e;
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

	private void z_distribute() {
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				distMap.forEach((String key, String value) -> {
					int h = key.hashCode();
					int d = Math.abs(h) % distMap.placeGroup().size();
					distMap.moveAtSync(key, distMap.placeGroup().get(d), mm); // Place with  rank `d`, not the Place with id==d
				});
				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on "+ here());
				e.printStackTrace();
				throw e;
			}
		});
	}
}

