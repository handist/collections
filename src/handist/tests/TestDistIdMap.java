package handist.tests;

import static apgas.Constructs.*;

import java.util.Random;
import java.util.ArrayList;

import apgas.Place;
import handist.util.dist.DistIdMap;
import handist.util.dist.MoveManagerLocal;
import handist.util.dist.TeamedPlaceGroup;

public class TestDistIdMap  {
    TeamedPlaceGroup placeGroup;
    // long numData = 10;
    long numData = 200;
    
    Random random;
    private DistIdMap<String> distIdMap;
    
    public TestDistIdMap(TeamedPlaceGroup placeGroup) {
        this.placeGroup = placeGroup;
        this.random = new Random(12345);
        this.distIdMap = new DistIdMap<String>(placeGroup);
    }
    
    public static void main(String[] args) {
        TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();
        new TestDistIdMap(world).run();
	System.out.println("----finish");
    }
    
    public String genRandStr(String header) {
        long rand = random.nextLong();
        return header + rand;
    }
    
    public void run() {
        // Create initial data at Place 0
	final TeamedPlaceGroup pg = placeGroup;
	final DistIdMap<String> distIdMap2 = this.distIdMap;
	finish(()->{
		pg.broadcastFlat(() -> {
			System.out.println("hello:" + here() + ", " + pg);
		    });
	    });
	
        System.out.println("### Create initial data at Place 0");
	
	try {
	    for (long i = 0; i < numData; i++) {
		distIdMap2.put(i, genRandStr("v"));
	    }
	} catch (Exception e) {
	    System.err.println("Error on "+here());
	    e.printStackTrace();
	}
	
	//	val gather = new GatherDistIdMap[String](pg, distIdMap2);
	//	gather.gather();
	//	gather.print();
	//	gather.setCurrentAsInit();
	
        // Distribute all entries
        System.out.println("");
        System.out.println("### MoveAtSync // Distribute all entries");
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    distIdMap2.forEach((Long key, String value) -> {
			    int d = (int) (key % pg.size());
			    System.out.println("" + here() + " moves key: " + key + " to " + d);
			    //distIdMap2.moveAtSync(key, pg.places().get(d), mm);
			    distIdMap2.moveAtSync(key, pg.get(d), mm); // Place with  rank `d`, not the Place with id==d
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
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//		val d = key % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 1-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 1-1: FAIL");
	//	}
	
	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Distribute all entries");
	//	pg.broadcastFlat(() => {
	//	    distIdMap2.updateDist();
	//	});
	
	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    Console.OUT.println("VALIDATE 1-2: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 1-2: FAIL");
	//	}

	
	// ---------------------------------------------------------------------------

	// Move all entries to the next place

	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to the next place");
	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    //val destination = Place.places().next(here);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank+1==pg.size()? 0: rank);

		    distIdMap2.forEach((Long key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distIdMap2.moveAtSync(key, destination, mm);
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
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//		val d = (key + 1) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 2-1: FAIL");
	//	}

	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Move all entries to the next place");
	//	pg.broadcastFlat(() => {
	//	    distIdMap2.updateDist();
	//	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    Console.OUT.println("VALIDATE 2-2: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 2-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

	// Move all entries to the next to next place

	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to the next to next place");
	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    //val destination = Place.places().next(here);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank+1==pg.size()? 0: rank);

		    distIdMap2.forEach((Long key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distIdMap2.moveAtSync(key, destination, mm);
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
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//		val d = (key + 3) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 3-1: FAIL");
	//	}

	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Move all entries to the next to next place");
	//	pg.broadcastFlat(() => {
	//	    distIdMap2.updateDist();
	//	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    Console.OUT.println("VALIDATE 3-2: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 3-2: FAIL");
	//	}

      
	// ---------------------------------------------------------------------------

	// Move all entries to the NPLACES times next place

	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to the NPLACES times next place");
	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    //val destination = Place.places().next(here);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank+1==pg.size()? 0: rank);
		    for (int i = 0; i < pg.size(); i++) {
			distIdMap2.forEach((Long key, String value) -> {
				System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
				distIdMap2.moveAtSync(key, destination, mm);
			    });
			mm.sync();
		    }

		} catch (Exception e) {
		    System.err.println("Error on "+here());
		    e.printStackTrace();
		    throw e;
		}
	    });


	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() &&
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//		val d = (key + 3) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 4-1: FAIL");
	//	}

	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Move all entries to the NPLACES times next place");
	//	pg.broadcastFlat(() => {
	//	    distIdMap2.updateDist();
	//	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    Console.OUT.println("VALIDATE 4-2: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 4-2: FAIL");
	//	}

      
	// ---------------------------------------------------------------------------

	// Move all entries to place 0
	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to place 0");
	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    Place destination = pg.get(0);
		    distIdMap2.forEach((Long key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distIdMap2.moveAtSync(key, destination, mm);
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
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//		return 0n;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 5-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 5-1: FAIL");
	//	}

	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Move all entries to place 0");
	//	pg.broadcastFlat(() => {
	//	    distIdMap2.updateDist();
	//	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    Console.OUT.println("VALIDATE 5-2: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 5-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

	// Generate additional key/value pair

	try {
	    for (long i = numData; i < numData * 2; i++) {
		distIdMap2.put(i, genRandStr("v"));
	    }
	} catch (Exception e) {
	    System.err.println("Error on "+here());
	    e.printStackTrace();
	}

	// Distribute all entries with additional key/value
        System.out.println("");
        System.out.println("### Distribute all entries with additional key/value");
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    distIdMap2.forEach((Long key, String value) -> {
			    int d = (int) (key % pg.size());
			    System.out.println("" + here() + " moves key: " + key + " to " + d);
			    distIdMap2.moveAtSync(key, pg.get(d), mm);
			});
		    mm.sync();
		} catch (Exception e) {
		    System.err.println("Error on "+here());
		    e.printStackTrace();
		    throw e;
		}
	    });

	// Then remove additional key/value
	final long numData2 = this.numData;
        System.out.println("");
        System.out.println("### Then remove additional key/value");
        pg.broadcastFlat(() -> {
		ArrayList<Long> keyList = new ArrayList<Long>();
		try {
		    //MoveManagerLocal mm = new MoveManagerLocal(pg);
		    distIdMap2.forEach((Long key, String value) -> {
			    if (key >= numData2) {
				System.out.println("[" + here() + "] try to remove " + key);
				keyList.add(key);
			    }
			});
		    for (long key : keyList) {
			distIdMap2.remove(key);
		    }
		} catch (Exception e) {
		    System.err.println("Error on "+here());
		    e.printStackTrace();
		    throw e;
		}
	    });

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() &&
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//		val d = key % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 6-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 6-1: FAIL");
	//	}

	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Distribute all entries again and remove additional data");
	//	pg.broadcastFlat(() => {
	//	    distIdMap2.updateDist();
	//	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    Console.OUT.println("VALIDATE 6-2: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 6-2: FAIL");
	//	}

    }
}

