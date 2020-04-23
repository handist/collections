package handist.collections.dist;

import static apgas.Constructs.*;

import java.util.Random;

import apgas.Place;
import handist.collections.dist.DistMap;
import handist.collections.dist.MoveManagerLocal;
import handist.collections.dist.TeamedPlaceGroup;

public class TestDistMap {
    TeamedPlaceGroup placeGroup;
    // long numData = 10;
    long numData = 200;

    Random random;
    private DistMap<String, String> distMap;

    public TestDistMap(TeamedPlaceGroup placeGroup) {
        this.placeGroup = placeGroup;
        this.random = new Random(12345);
        this.distMap = new DistMap<String, String>(placeGroup);
    }

    public static void main(String[] args) {
        TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();
	new TestDistMap(world).run();
	System.out.println("----finish");

    }

    public String genRandStr(String header) {
        long rand = random.nextLong();
        return header + rand;
    }

    public void run() {
        // Create initial data at Place 0
	final TeamedPlaceGroup pg = this.placeGroup;
	final DistMap<String,String> distMap2 = this.distMap;
	finish(()->{
		pg.broadcastFlat(() -> {
			System.out.println("hello:" + here() + ", " + pg);
		    });
	    });
	
        System.out.println("### Create initial data at Place 0");

        for (int i=0; i<numData; i++) {
            distMap2.put(genRandStr("k"), genRandStr("v"));
        }

	//  	val gather = new GatherDistMap[String, String](pg, distMap2);
	//  	gather.gather();
	//  	gather.print();
	//  	gather.setCurrentAsInit();
	
        // Distribute all entries
        System.out.println("");
        System.out.println("### MoveAtSync // Distribute all entries");
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    distMap2.forEach((String key, String value) -> {
			    int h = key.hashCode();
			    int d = Math.abs(h) % pg.size();
			    System.out.println("" + here() + " moves key: " + key + " to " + d);
			    //distMap2.moveAtSync(key, pg.places().get(d), mm);
			    distMap2.moveAtSync(key, pg.get(d), mm); // Place with  rank `d`, not the Place with id==d
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
	//	    gather.validateLocationAndValue((key: String, pid: Int) => {
	//		val h = key.hashCode() as Long;
	//		val d = Math.abs(h) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 1-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 1-1: FAIL");
	//	}
	//

	// ---------------------------------------------------------------------------
	// Move all entries to the next place
	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to the next place");

	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    //val destination = Place.places().next(here);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
		    distMap2.forEach((String key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distMap2.moveAtSync(key, destination, mm);
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
	//	    gather.validateLocationAndValue((key: String, pid: Int) => {
	//		val h = key.hashCode() as Long;
	//		val d = (Math.abs(h) + 1) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 2-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 2-1: FAIL");
	//	}
	//

	// ---------------------------------------------------------------------------
	// Move all entries to the next to next place
	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to the next to next place");

	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    //val destination = Place.places().next(here);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
		    
		    distMap2.forEach((String key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distMap2.moveAtSync(key, destination, mm);
			});
		    mm.sync();
		    distMap2.forEach((String key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distMap2.moveAtSync(key, destination, mm);
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
	//	    gather.validateLocationAndValue((key: String, pid: Int) => {
	//		val h = key.hashCode() as Long;
	//		val d = (Math.abs(h) + 3) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 3-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 3-1: FAIL");
	//	}
	//

	// ---------------------------------------------------------------------------
	// Move all entries to place 0
	System.out.println("");
	System.out.println("### MoveAtSync // Move all entries to place 0");
	pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    Place destination = pg.get(0);
		    distMap2.forEach((String key, String value) -> {
			    System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
			    distMap2.moveAtSync(key, destination, mm);
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
	//	    gather.validateLocationAndValue((key: String, pid: Int) => {
	//		return 0n;
	//	    })) {
	//	    System.out.println("VALIDATE 4-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 4-1: FAIL");
	//	}
	//
    }
}

