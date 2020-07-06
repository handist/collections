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

import java.io.Serializable;
import java.lang.Math;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;

import apgas.Place;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=2, launcher=TestLauncher.class)
public class IT_DistMapList implements Serializable {
	
	/** Serial Version UID */
	private static final long serialVersionUID = -699276324622147605L;
	
	TeamedPlaceGroup placeGroup;
	long numData = 200;
	long numKey = 20;
	long NPLACES;
	DistMapList<String, String> distMapList;
	ArrayList<String> keyList;
	Random random;

	@Before
	public void setup() {
		placeGroup = TeamedPlaceGroup.getWorld();
		NPLACES = placeGroup.size();
		random = new Random(12345);
		distMapList = new DistMapList<String, String>(placeGroup);
		keyList = new ArrayList<String>();
	}

	public static void main(String[] args) {
		IT_DistMapList test = new IT_DistMapList();
		test.setup();
		test.run();
		System.out.println("----finish");
	}

	public String genRandStr(String header) {
		long rand = random.nextLong();
		return header + rand;
	}

	@Test
	public void run() {
		final TeamedPlaceGroup pg = placeGroup;	
		finish(()->{
			placeGroup.broadcastFlat(() -> {
				System.out.println("hello:" + here() + ", " + pg);
			});
		});	

		// Create initial data at Place 0
		System.out.println("### Create initial data at Place 0");

		try {
			for (long i = 0; i < numKey; i++) {
				keyList.add(genRandStr("r"));
			}
			long j = 0;
			for (long i = 0; i < numData; i++) {
				distMapList.put1(keyList.get((int)j), genRandStr("v"));
				j = (j + 1) % numKey;
			}
		} catch (Exception e) {
			System.err.println("Error on "+here());
			e.printStackTrace();
		}


		//	val gather = new GatherDistMapList[String, String](placeGroup, distMapList);
		//	gather.gather();
		//	gather.print();
		//	gather.setCurrentAsInit();

		// Distribute all entries
		final DistMapList<String,String> distMapList2 = this.distMapList;
		final int NPLACES2 = pg.size;
		System.out.println("");
		System.out.println("### MoveAtSync // Distribute all entries");
		pg.broadcastFlat(() -> {
			try {
				System.out.println("broadcast Flat test0:" +here());		    
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				distMapList2.forEach1((String key, String value) -> {
					int h = key.hashCode();
					int d = (int)(Math.abs(h) % NPLACES2);
					System.out.println("" + here() + " moves key: " + key + " to " + d);
					distMapList2.moveAtSync(key, pg.get(d), mm);
				});
				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on "+here());
				e.printStackTrace();
				throw e;
			}
		});

		//	gather.gather();
		//	gather.print();
		//	if (gather.validate() &&
		//	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		//		val h = key.hashCode() as Long;
		//		val d = Math.abs(h) % NPLACES2;
		//		return d as Int;
		//	    })) {
		//	    System.out.println("VALIDATE: SUCCESS");
		//	} else {
		//	    System.out.println("VALIDATE: FAIL");
		//	}

		// ------------------------------------------------------------------------------

		// Move all entries to the next place
		System.out.println("");
		System.out.println("### MoveAtSync // Move all entries to the next place");
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				int rank = pg.rank(here());
				Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);

				distMapList2.forEach1((String key, String value) -> {
					System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
					distMapList2.moveAtSync(key, destination, mm);
				});

				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on "+here());
				e.printStackTrace();
				throw e;
			}
		});

		//	gather.gather();
		//	gather.print();
		//	if (gather.validate() &&
		//	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		//		val h = key.hashCode() as Long;
		//		val d = (Math.abs(h) + 1) % NPLACES2;
		//		return d as Int;
		//	    })) {
		//	    System.out.println("VALIDATE: SUCCESS");
		//	} else {
		//	    System.out.println("VALIDATE: FAIL");
		//	}

		// ---------------------------------------------------------------------------

		// Add new data on Place 0
		System.out.println("### Add new data on Place 0");

		try {
			long j = numData % numKey;
			for (long i = 0; i < numData; i++) {
				distMapList2.put1(keyList.get((int)j), genRandStr("x"));
				j = (j + 1) % numKey;
			}
		} catch (Exception e) {
			System.err.println("Error on "+here());
			e.printStackTrace();
		}

		//	gather.gather();
		//	gather.print();
		//	gather.setCurrentAsInit();

		// Move entries on even number place to the next odd number place
		System.out.println("");
		System.out.println("### MoveAtSync // Move entries on even number place to the next odd number place");
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				int rank = pg.rank(here());
				Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
				if (here().id % 2 == 0) {
					distMapList2.forEach1((String key, String value) -> {
						System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
						distMapList2.moveAtSync(key, destination, mm);
					});
				}
				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				throw e;
			}
		});

		//	gather.gather();
		//	gather.print();
		//	if (gather.validate() &&
		//	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		//		if (value.startsWith("v")) {
		//		    val h = key.hashCode() as Long;
		//		    val prev = (Math.abs(h) + 1) % NPLACES2;
		//		    if (prev % 2 == 0) {
		////		        System.out.println("v even " + prev);
		//		        return ((prev + 1) % NPLACES2) as Int;
		//		    } else {
		////		        System.out.println("v odd " + prev);
		//		        return prev as Int;
		//		    }
		//		} else {
		//		    // value.startsWith("x")
		//		    if (pid as Long % 2  == 0) {
		////		        System.out.println("x even " + pid);
		//		        val d = (pid as Long + 1) % NPLACES2;
		//			return d as Int;
		//		    } else {
		////		        System.out.println("x odd " + pid);
		//		        return pid;
		//		    }
		//		}
		//	    })) {
		//	    System.out.println("VALIDATE: SUCCESS");
		//	} else {
		//	    System.out.println("VALIDATE: FAIL");
		//	}


		// ---------------------------------------------------------------------------

		// Move all entries to place 0
		System.out.println("");
		System.out.println("### MoveAtSync // Move all entries to place 0");
		pg.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(pg);
				Place destination = pg.get(0);
				distMapList2.forEach1((String key, String value) -> {
					System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
					distMapList2.moveAtSync(key, destination, mm);
				});
				mm.sync();
			} catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				throw e;
			}
		});

		//	gather.gather();
		//	gather.print();
		//	if (gather.validate() &&
		//	    gather.validateLocationOfKeyValue((key: String, value: String, pid: Int) => {
		//		return 0n;
		//	    })) {
		//	    System.out.println("VALIDATE: SUCCESS");
		//	} else {
		//	    System.out.println("VALIDATE: FAIL");
		//	}

	}
}
