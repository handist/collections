package handist.tests;

import static apgas.Constructs.*;

import java.util.Random;
import java.util.ArrayList;
import java.io.Serializable;

import apgas.Place;
import handist.util.dist.DistIdMap;
import handist.util.dist.MoveManagerLocal;
import handist.util.dist.TeamedPlaceGroup;

public class TestDistIdMap implements Serializable {
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
    }
    
    public String genRandStr(String header) {
        long rand = random.nextLong();
        return header + rand;
    }
    
    public void run() {
        // Create initial data at Place 0
	finish(()->{
		placeGroup.broadcastFlat(() -> {
			System.out.println("hello:" + here() + ", " + placeGroup);
		    });
	    });
	
        System.out.println("### Create initial data at Place 0");
	
	try {
	    for (long i = 0; i < numData; i++) {
		distIdMap.put(i, genRandStr("v"));
	    }
	} catch (Exception e) {
	    System.err.println("Error on "+here());
	    e.printStackTrace();
	}
	
	//	val gather = new GatherDistIdMap[String](placeGroup, distIdMap);
	//	gather.gather();
	//	gather.print();
	//	gather.setCurrentAsInit();
	
        // Distribute all entries
        System.out.println("");
        System.out.println("### MoveAtSync // Distribute all entries");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    distIdMap.forEach((Long key, String value) -> {
			    int d = (int) (key % placeGroup.size());
			    System.out.println("" + here() + " moves key: " + key + " to " + d);
			    //distIdMap.moveAtSync(key, placeGroup.places().get(d), mm);
			    distIdMap.moveAtSync(key, placeGroup.get(d), mm); // Place with  rank `d`, not the Place with id==d
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
	//	placeGroup.broadcastFlat(() => {
	//	    distIdMap.updateDist();
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
	placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    //val destination = Place.places().next(here);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(rank+1==placeGroup.size()? 0: rank);

		    distIdMap.forEach((Long key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distIdMap.moveAtSync(key, destination, mm);
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
	//	placeGroup.broadcastFlat(() => {
	//	    distIdMap.updateDist();
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
	placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    //val destination = Place.places().next(here);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(rank+1==placeGroup.size()? 0: rank);

		    distIdMap.forEach((Long key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distIdMap.moveAtSync(key, destination, mm);
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
	//	placeGroup.broadcastFlat(() => {
	//	    distIdMap.updateDist();
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
	placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    //val destination = Place.places().next(here);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(rank+1==placeGroup.size()? 0: rank);
		    for (int i = 0; i < placeGroup.size(); i++) {
			distIdMap.forEach((Long key, String value) -> {
				System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
				distIdMap.moveAtSync(key, destination, mm);
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
	//	placeGroup.broadcastFlat(() => {
	//	    distIdMap.updateDist();
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
	placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    Place destination = placeGroup.get(0);
		    distIdMap.forEach((Long key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distIdMap.moveAtSync(key, destination, mm);
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
	//		return 0n;
	//	    })) {
	//	    Console.OUT.println("VALIDATE 5-1: SUCCESS");
	//	} else {
	//	    Console.OUT.println("VALIDATE 5-1: FAIL");
	//	}

	//        Console.OUT.println("");
	//        Console.OUT.println("### Update dist // Move all entries to place 0");
	//	placeGroup.broadcastFlat(() => {
	//	    distIdMap.updateDist();
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
		distIdMap.put(i, genRandStr("v"));
	    }
	} catch (Exception e) {
	    System.err.println("Error on "+here());
	    e.printStackTrace();
	}

	// Distribute all entries with additional key/value
        System.out.println("");
        System.out.println("### Distribute all entries with additional key/value");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    distIdMap.forEach((Long key, String value) -> {
			    int d = (int) (key % placeGroup.size());
			    System.out.println("" + here() + " moves key: " + key + " to " + d);
			    distIdMap.moveAtSync(key, placeGroup.get(d), mm);
			});
		    mm.sync();
		} catch (Exception e) {
		    System.err.println("Error on "+here());
		    e.printStackTrace();
		    throw e;
		}
	    });

	// Then remove additional key/value
        System.out.println("");
        System.out.println("### Then remove additional key/value");
        placeGroup.broadcastFlat(() -> {
		ArrayList<Long> keyList = new ArrayList<Long>();
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    distIdMap.forEach((Long key, String value) -> {
			    if (key >= numData) {
				System.out.println("[" + here() + "] try to remove " + key);
				keyList.add(key);
			    }
			});
		    for (long key : keyList) {
			distIdMap.remove(key);
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
	//	placeGroup.broadcastFlat(() => {
	//	    distIdMap.updateDist();
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

