package handist.collections.dist;

import static apgas.Constructs.*;

import java.util.List;
import java.util.ArrayList;

import apgas.Place;
import handist.collections.LongRange;
import handist.collections.Chunk;
import handist.collections.RangedList;
import handist.collections.dist.DistCol;
import handist.collections.dist.DistBag;
import handist.collections.dist.MoveManagerLocal;
import handist.collections.dist.TeamedPlaceGroup;


public class TestDistCol {
    TeamedPlaceGroup placeGroup;
    long NPLACES0;
    long rangeSize0 = 10;
    long rangeSkip0 = 5;
    long numChunk0 = 50;

    DistCol<String> distCol0;
    DistBag<List<String> > distBag0;

    public TestDistCol(TeamedPlaceGroup placeGroup) {
        this.placeGroup = placeGroup;
	NPLACES0 = placeGroup.size();
	distCol0 = new DistCol<String>(placeGroup);
	distBag0 = new DistBag<List<String> >(placeGroup);
    }

    public static void main(String[] args) {
        TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();
	new TestDistCol(world).run();
    }

    public void run() {
	TeamedPlaceGroup pg = this.placeGroup;
	long NPLACES =  NPLACES0;
	long rangeSize = rangeSize0;
	long rangeSkip = rangeSkip0;
	long numChunk = numChunk0;
	
	DistCol<String> distCol=distCol0;
	DistBag<List<String> > distBag=distBag0;
	
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

	//	val gather = new GatherDistCol[String](pg, distCol);
	//	gather.gather();
	//	gather.print();
	//	gather.setCurrentAsInit();

	// ---------------------------------------------------------------------------

        // Distribute all entries
        System.out.println("");
        System.out.println("### MoveAtSync // Distribute all entries");
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    String s = c.get(r.from);
			    long d = (Long.parseLong(s.split("/")[0])) % NPLACES;
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.from + ".." + r.to + ") to " + d);
			    try {
				distCol.moveAtSync(cs, pg.get((int)d), mm);
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
	pg.broadcastFlat(() -> {
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
        pg.broadcastFlat(() -> {
		//		System.out.println("Line 147:"+distCol.ldist.toString() +"@"+here());

		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
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
	pg.broadcastFlat(() -> {
		try {
		    // System.out.println("BeforeUpdateDist: "+distCol.ldist.toString() +"@"+here());		    
		    distCol.updateDist();
		    // System.out.println("AfterUpdateDist: "+distCol.ldist.toString() +"@"+here());
		    
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
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
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
			    System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
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
	pg.broadcastFlat(() -> {
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
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    int rank = pg.rank(here());
		    Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
		    for (long i = 0; i < NPLACES; i++) {
			distCol.forEachChunk((RangedList<String> c) -> {
				LongRange r = c.getRange();
				ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
				cs.add(c);
				System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
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
	pg.broadcastFlat(() -> {
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
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    //int rank = pg.rank(here());
		    Place destination = pg.get(0);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
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
	pg.broadcastFlat(() -> {
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
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    String s = c.get(r.from);
			    long d = (Long.parseLong(s.split("/")[0])) % NPLACES;
			    ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
			    cs.add(c);
			    System.out.println("[" + r.from + ".." + r.to + ") to " + d);
			    try {
				distCol.moveAtSync(cs, pg.get((int)d), mm);
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
        pg.broadcastFlat(() -> {
		try {
		    ArrayList<RangedList<String> > chunkList = new ArrayList<RangedList<String> >();
		    distCol.forEachChunk((RangedList<String> c) -> {
			    LongRange r = c.getRange();
			    if (r.from / (rangeSize + rangeSkip) >= numChunk) {
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
	pg.broadcastFlat(() -> {
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
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    LongRange range = new LongRange(0, splitSizeLarge);
		    long dest = 0;
		    while (range.from < AllRange.to) {
			distCol.moveAtSync(range, pg.get((int)dest), mm);
			range = new LongRange(range.from + splitSizeLarge, range.to + splitSizeLarge);
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
	pg.broadcastFlat(() -> {
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
        pg.broadcastFlat(() -> {
		try {
		    MoveManagerLocal mm = new MoveManagerLocal(pg);
		    LongRange range = new LongRange(0, splitSizeSmall);
		    long dest = 0;
		    while (range.from < AllRange.to) {
			distCol.moveAtSync(range, pg.get((int)dest), mm);
			range = new LongRange(range.from + splitSizeSmall, range.to + splitSizeSmall);
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
	pg.broadcastFlat(() -> {
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
