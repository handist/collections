package handist.tests;

import static apgas.Constructs.*;

import java.util.Random;
import java.io.Serializable;

import apgas.Place;
import handist.util.dist.DistMap;
import handist.util.dist.MoveManagerLocal;
import handist.util.dist.TeamedPlaceGroup;

public class TestDistMap implements Serializable {
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

        for (int i=0; i<numData; i++) {
            distMap.put(genRandStr("k"), genRandStr("v"));
        }

//  	val gather = new GatherDistMap[String, String](placeGroup, distMap);
//  	gather.gather();
//  	gather.print();
//  	gather.setCurrentAsInit();

        // Distribute all entries
        System.out.println("");
        System.out.println("### MoveAtSync // Distribute all entries");
        placeGroup.broadcastFlat(() -> {
            try {
                MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		distMap.forEach((String key, String value) -> {
                    int h = key.hashCode();
                    int d = Math.abs(h) % placeGroup.size();
                    System.out.println("" + here() + " moves key: " + key + " to " + d);
                    //distMap.moveAtSync(key, placeGroup.places().get(d), mm);
		    distMap.moveAtSync(key, placeGroup.get(d), mm); // Place with  rank `d`, not the Place with id==d
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
//	    Console.OUT.println("VALIDATE 1-1: SUCCESS");
//	} else {
//	    Console.OUT.println("VALIDATE 1-1: FAIL");
//	}
//
      // Move all entries to the next place

      System.out.println("");
      System.out.println("### MoveAtSync // Move all entries to the next place");
      placeGroup.broadcastFlat(() -> {
          try {
              MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
              //val destination = Place.places().next(here);
              int rank = placeGroup.rank(here());
              Place destination = placeGroup.get(rank+1==placeGroup.size()? 0: rank);

              distMap.forEach((String key, String value) -> {
                  System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
                  distMap.moveAtSync(key, destination, mm);
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
//	    Console.OUT.println("VALIDATE 2-1: SUCCESS");
//	} else {
//	    Console.OUT.println("VALIDATE 2-1: FAIL");
//	}
//
//      // Move all entries to the next to next place
//      Console.OUT.println("");
//      Console.OUT.println("### MoveAtSync // Move all entries to the next to next place");
//      Place.places().broadcastFlat(() => {
//	    val mm = new MoveManagerLocal(placeGroup, team);
//	    val destination = Place.places().next(here);
//	    distMap.each((key: String, value: String) => {
//		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
//		distMap.moveAtSync(key, destination, mm);
//	    });
//          mm.sync();
//	    distMap.each((key: String, value: String) => {
//		Console.OUT.println("" + here + " moves key: " + key + " to " + destination.id);
//		distMap.moveAtSync(key, destination, mm);
//	    });
//          mm.sync();
//      });
//
//	gather.gather();
//	gather.print();
//	if (gather.validate() &&
//	    gather.validateLocationAndValue((key: String, pid: Int) => {
//		val h = key.hashCode() as Long;
//		val d = (Math.abs(h) + 3) % NPLACES;
//		return d as Int;
//	    })) {
//	    Console.OUT.println("VALIDATE 3-1: SUCCESS");
//	} else {
//	    Console.OUT.println("VALIDATE 3-1: FAIL");
//	}
//
      // Move all entries to place 0
      System.out.println("");
      System.out.println("### MoveAtSync // Move all entries to place 0");
      placeGroup.broadcastFlat(() -> {
          try {
              MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
              Place destination = placeGroup.get(0);
              distMap.forEach((String key, String value) -> {
                  System.out.println("" + here() + " moves key: " + key + " to " + destination.id);
                  distMap.moveAtSync(key, destination, mm);
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
//	    Console.OUT.println("VALIDATE 4-1: SUCCESS");
//	} else {
//	    Console.OUT.println("VALIDATE 4-1: FAIL");
//	}
//
  }
}

