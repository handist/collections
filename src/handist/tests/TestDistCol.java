package handist.tests;

import static apgas.Constructs.*;

import java.util.List;
import java.util.ArrayList;
import java.io.Serializable;

import apgas.Place;
import handist.util.LongRange;
import handist.util.Chunk;
import handist.util.RangedList;
import handist.util.dist.DistCol;
import handist.util.dist.DistBag;
import handist.util.dist.MoveManagerLocal;
import handist.util.dist.TeamedPlaceGroup;


public class TestDistCol implements Serializable {
    TeamedPlaceGroup placeGroup;
    long NPLACES;
    long rangeSize = 10;
    long rangeSkip = 5;
    long numChunk = 50;

    DistCol<String> distCol;
    DistBag<List<String> > distBag;

    public TestDistCol(TeamedPlaceGroup placeGroup) {
        this.placeGroup = placeGroup;
	NPLACES = placeGroup.size();
	distCol = new DistCol<String>(placeGroup);
	distBag = new DistBag<List<String> >(placeGroup);
    }

    public static void main(String[] args) {
        TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();
	new TestDistCol(world).run();
    }

    public void run() {

        // Create initial data at Place 0
        System.out.println("### Create initial data at Place 0");

	long rangeBegin = 0; // inclusive
	long rangeEnd; // exclusive

	try {
	    for (long i = 0; i < numChunk; i++) {
		rangeEnd = rangeBegin + rangeSize - 1;
		Chunk<String> c = new Chunk<String>(new LongRange(rangeBegin, rangeEnd), "<empty>");
		for (long j = rangeBegin; j < rangeEnd; j++) {
		    c.set(j, "" + j + "/" + i);
		}
		distCol.putChunk(c);
		rangeBegin = rangeBegin + rangeSize + rangeSkip;
	    }
	} catch (Exception e) {
	    System.err.println("Error on "+here());
	    e.printStackTrace();
	}

	//	val gather = new GatherDistCol[String](placeGroup, distCol);
	//	gather.gather();
	//	gather.print();
	//	gather.setCurrentAsInit();

	// ---------------------------------------------------------------------------

        // Distribute all entries
        System.out.println("");
        System.out.println("### MoveAtSync // Distribute all entries");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    String s = c.get(r.begin);
			    long d = (Long.parseLong(s.split("/")[0])) % NPLACES;
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.begin + ".." + r.end + "] to " + d);
			    try {
				distCol.moveAtSync(cs, placeGroup.get((int)d), mm);
			    } catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				//throw e;
			    }
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
	//	        val cblock = key / (rangeSize + rangeSkip);
	//		val d = cblock % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 1-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 1-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Distribute all entries");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 1-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 1-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

        // Move all entries to the next place
        System.out.println("");
        System.out.println("### MoveAtSync // Move all entries to the next place");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(rank + 1 == placeGroup.size() ? 0 : rank);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.begin + ".." + r.end + "] to " + destination.id);
			    try {
				distCol.moveAtSync(cs, destination, mm);
			    } catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				//throw e;
			    }
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
	//	        val cblock = key / (rangeSize + rangeSkip);
	//		val d = (cblock + 1) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 2-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 2-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Move all entries to the next place");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 2-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 2-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

        // Move all entries to the next to next place
        System.out.println("");
        System.out.println("### MoveAtSync // Move all entries to the next to next place");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(rank + 1 == placeGroup.size() ? 0 : rank);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.begin + ".." + r.end + "] to " + destination.id);
			    try {
				distCol.moveAtSync(cs, destination, mm);
			    } catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				//throw e;
			    }
			});
		    mm.sync();
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.begin + ".." + r.end + "] to " + destination.id);
			    try {
				distCol.moveAtSync(cs, destination, mm);
			    } catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				//throw e;
			    }
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
	//	        val cblock = key / (rangeSize + rangeSkip);
	//		val d = (cblock + 3) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 3-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 3-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Move all entries to the next to next place");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 3-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 3-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

        // Move all entries to the NPLACES times next place
        System.out.println("");
        System.out.println("### MoveAtSync // Move all entries to the NPLACES times next place");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(rank + 1 == placeGroup.size() ? 0 : rank);
		    for (long i = 0; i < NPLACES; i++) {
			distCol.forEachChunk((RangedList<String> c) -> {
				LongRange r = c.getRange();
				ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
				cs.add(c);
				System.out.println("[" + r.begin + ".." + r.end + "] to " + destination.id);
				try {
				    distCol.moveAtSync(cs, destination, mm);
				} catch (Exception e) {
				    System.err.println("Error on " + here());
				    e.printStackTrace();
				    //throw e;
				}
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
	//	        val cblock = key / (rangeSize + rangeSkip);
	//		val d = (cblock + 3) % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 4-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 4-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Move all entries to the NPLACES times next place");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 4-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 4-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

        // Move all entries to place 0
        System.out.println("");
        System.out.println("### MoveAtSync // Move all entries to place 0");
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    int rank = placeGroup.rank(here());
		    Place destination = placeGroup.get(0);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.begin + ".." + r.end + "] to " + destination.id);
			    try {
				distCol.moveAtSync(cs, destination, mm);
			    } catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				//throw e;
			    }
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
	//    	})) {
	//	    System.out.println("VALIDATE 5-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 5-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Move all entries to place 0");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 5-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 5-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------

	// Generate additional key/value pair
	try {
	    for (long i = numChunk; i < numChunk * 2; i++) {
		rangeEnd = rangeBegin + rangeSize;
		Chunk<String> c = new Chunk<String>(new LongRange(rangeBegin, rangeEnd), "<empty>");
		for (long j = rangeBegin; j < rangeEnd; j++) {
		    c.set(j, "" + j + "/" + i);
		}
		distCol.putChunk(c);
		rangeBegin = rangeBegin + rangeSize + rangeSkip;
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
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    String s = c.get(r.begin);
			    long d = (Long.parseLong(s.split("/")[0])) % NPLACES;
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.begin + ".." + r.end + "] to " + d);
			    try {
				distCol.moveAtSync(cs, placeGroup.get((int)d), mm);
			    } catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				//throw e;
			    }
			});
		    mm.sync();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
        });

	// Then remove additional key/value
        System.out.println("");
        System.out.println("### Then remove additional key/value");
        placeGroup.broadcastFlat(() -> {
		try {
		    ArrayList<RangedList<String> > chunkList = new ArrayList<RangedList<String> >();
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    if (r.begin / (rangeSize + rangeSkip) >= numChunk) {
				chunkList.add(c);
			    }
			});	    
		    for (RangedList<String> chunk : chunkList) {
			distCol.removeChunk(chunk);
		    }
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
	//	        val cblock = key / (rangeSize + rangeSkip);
	//		val d = cblock % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 6-1: SUCCESS");
	//	} else {
       	//	    System.out.println("VALIDATE 6-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Distribute all entries again and remove additional data");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
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
	//	        val cblock = key / (rangeSize + rangeSkip);
	//		val d = cblock % NPLACES;
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 6-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 6-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------
	// split range into large pieces
	long splitSizeLarge = rangeSize * (numChunk / 3);
	LongRange AllRange = new LongRange(0, ((rangeSize + rangeSkip) * numChunk));

        System.out.println("");
        System.out.println("### Split range into large pieces splitSizeLarge: " + splitSizeLarge);
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    LongRange range = new LongRange(0, splitSizeLarge);
		    long dest = 0;
		    while (range.begin < AllRange.end) {
			distCol.moveAtSync(range, placeGroup.get((int)dest), mm);
			range = new LongRange(range.begin + splitSizeLarge, range.end + splitSizeLarge);
			dest = (dest + 1) % NPLACES;
		    }
		    mm.sync();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    //throw e;
		}
	    });
	
	//	gather.gather();
	//	gather.print();

	//	if (gather.validate() &&
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//	        val d = (key / splitSizeLarge) % NPLACES;	    
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 7-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 7-1: FAIL");
	//	}


        System.out.println("");
        System.out.println("### Update dist // Split range into large pieces");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 7-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 7-2: FAIL");
	//	}

	// ---------------------------------------------------------------------------
	// split range into small pieces
	long splitSizeSmall = 4;

        System.out.println("");
        System.out.println("### Split range into small pieces splitSizeSmall: " + splitSizeSmall);
        placeGroup.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
		    LongRange range = new LongRange(0, splitSizeSmall);
		    long dest = 0;
		    while (range.begin < AllRange.end) {
			distCol.moveAtSync(range, placeGroup.get((int)dest), mm);
			range = new LongRange(range.begin + splitSizeSmall, range.end + splitSizeSmall);
			dest = (dest + 1) % NPLACES;
		    }
		    mm.sync();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    //throw e;
		}
	    });
	
	//	gather.gather();
	//	gather.print();

	//	if (gather.validate() &&
	//	    gather.validateLocationAndValue((key: Long, pid: Int) => {
	//	        val d = (key / splitSizeSmall) % NPLACES;	    
	//		return d as Int;
	//	    })) {
	//	    System.out.println("VALIDATE 8-1: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 8-1: FAIL");
	//	}

        System.out.println("");
        System.out.println("### Update dist // Split range into small pieces");
	placeGroup.broadcastFlat(() -> {
		try {
		    distCol.updateDist();
		} catch (Exception e) {
		    System.err.println("Error on " + here());
		    e.printStackTrace();
		    throw e;
		}
	});

	//	gather.gather();
	//	gather.print();
	//	if (gather.validate() && gather.validateAfterUpdateDist()) {
	//	    System.out.println("VALIDATE 8-2: SUCCESS");
	//	} else {
	//	    System.out.println("VALIDATE 8-2: FAIL");
	//	}

    }
}
