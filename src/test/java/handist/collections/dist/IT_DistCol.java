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
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.function.SerializableFunction;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=2, launcher=TestLauncher.class)
public class IT_DistCol implements Serializable {

	/** Number of Chunks used in the test */
	static final long numChunk = 50;
	/** Range of each individual chunk */
	static final long rangeSize = 10;
	/** Size of the range skipped between chunks */
	static final long rangeSkip = 5;

	/** Serial Version UID */
	private static final long serialVersionUID = -9076195681727813858L;

	/** Object instance under test, initially empty */
	DistBag<List<String> > distBag;
	/** Object instance under test, initially empty */
	DistCol<String> distCol;
	/** Number of processes on which this test is running */
	int NPLACES;
	/** PlaceGroup representing the whole world */
	TeamedPlaceGroup placeGroup = TeamedPlaceGroup.getWorld();

	@Before
	public void setup() {
		placeGroup = TeamedPlaceGroup.world;
		NPLACES = placeGroup.size();
		distCol = new DistCol<String>(placeGroup);
		distBag = new DistBag<List<String> >(placeGroup);
	}

	@Test(timeout=100000)
	public void testRun() throws Throwable {
		// Prepare initial population
		long rangeBegin = 0; // inclusive
		long rangeEnd; // exclusive
		try {
			for (long i = 0; i < numChunk; i++) {
				rangeEnd = rangeBegin + rangeSize - 1;
				Chunk<String> c = new Chunk<String>(new LongRange(rangeBegin, rangeEnd), "<empty>");
				for (long j = rangeBegin; j < rangeEnd; j++) {
					c.set(j, "" + j + "/" + i);
				}
				distCol.add(c);
				rangeBegin = rangeBegin + rangeSize + rangeSkip;
			}
		} catch (Exception e) {
			System.err.println("Error on "+here());
			e.printStackTrace();
			throw e;
		}
		final long INITIAL_SIZE = distCol.size();

		// Check that the expected number of entries are indeed in DistCol
		try {
			placeGroup.broadcastFlat(()-> {
				long expected = placeGroup.rank(here()) == 0? INITIAL_SIZE : 0l;
				assertEquals(expected, distCol.size());				
			});
		} catch (MultipleException me) {
			throw me.getSuppressed()[0];
		}

		// Distribute all entries
		z_distributeChunks();

		// Check that each place got 1 out of 2 chunk
		x_checkSize((h)->{return INITIAL_SIZE/2;});
		//Check that the expected shift is correct
		x_checkShift(0l);

		// Move all entries to the next place
		z_updateDist();		
		z_moveToNextPlace();

		x_checkSize((h)->{return INITIAL_SIZE/2;});
		x_checkShift(1l);

		// Move all entries to the next-next place");
		z_updateDist();
		z_moveToNextPlace();
		z_moveToNextPlace();
		z_updateDist();

		x_checkSize((h)->{return INITIAL_SIZE/2;});
		x_checkShift(3l);

		// ---------------------------------------------------------------------------

		z_moveToNextPlace();

		x_checkSize((h)->{return INITIAL_SIZE/2;});
		x_checkShift(4l);

		z_updateDist();

		// ---------------------------------------------------------------------------
		// Move all entries to place 0
		z_moveToPlaceZero();

		//All the entries should be on place 0, size 0 elsewhere
		x_checkSize((h)->{return h.id == 0 ? INITIAL_SIZE: 0l;}); 

		z_updateDist();

		// ---------------------------------------------------------------------------
		// Generate additional key/value pair
		long newEntriesCount = 0l;
		for (long i = numChunk; i < numChunk * 2; i++) {
			rangeEnd = rangeBegin + rangeSize;
			Chunk<String> c = new Chunk<String>(new LongRange(rangeBegin, rangeEnd), "<empty>");
			for (long j = rangeBegin; j < rangeEnd; j++) {
				c.set(j, "" + j + "/" + i);
			}
			newEntriesCount += c.size();
			distCol.add(c);
			rangeBegin = rangeBegin + rangeSize + rangeSkip;
		}
		final long ADDED_ENTRIES = newEntriesCount;
		x_checkSize((h)->{return h.id == 0 ? INITIAL_SIZE + ADDED_ENTRIES: 0l;}); 

		// Distribute all entries with the additional keys/values
		z_distributeChunks();

		// CHECK THAT THE DISTRIBUTION IS CORRECT
		x_checkShift(0l);
		x_checkSize((h)->{return (INITIAL_SIZE + ADDED_ENTRIES)/2;}); 
		// Then remove the additional key/value
		placeGroup.broadcastFlat(() -> {
			try {
				ArrayList<RangedList<String> > chunkList = new ArrayList<RangedList<String> >();
				distCol.forEachChunk((RangedList<String> c) -> {
					LongRange r = c.getRange();
					if (r.from / (rangeSize + rangeSkip) >= numChunk) {
						chunkList.add(c);
					}
				});	    
				for (RangedList<String> chunk : chunkList) {
					distCol.remove(chunk.getRange());
				}
			} catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				throw e;
			}
		});

		z_updateDist();
		x_checkSize((h)->{return INITIAL_SIZE/2;});

		// ---------------------------------------------------------------------------
		// Split range into large ranges
		long splitSizeLarge = rangeSize * (numChunk / 3);
		LongRange AllRange = new LongRange(0, ((rangeSize + rangeSkip) * numChunk));

		placeGroup.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
				LongRange range = new LongRange(0, splitSizeLarge);
				long dest = 0;
				while (range.from < AllRange.to) {
					distCol.moveRangeAtSync(range, placeGroup.get((int)dest), mm);
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
		z_updateDist();
		// TODO check

		// ---------------------------------------------------------------------------
		// Split range into smaller ranges
		long splitSizeSmall = 4;
		placeGroup.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
				LongRange range = new LongRange(0, splitSizeSmall);
				long dest = 0;
				while (range.from < AllRange.to) {
					distCol.moveRangeAtSync(range, placeGroup.get((int)dest), mm);
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

		z_updateDist();

		//	TODO CHECK
	}

	/**
	 * Subroutine which checks that every place holds half of the total instances
	 * @param INITIAL_SIZE total size of the distributed collection
	 * @throws Throwable if thrown during the check
	 */
	private void x_checkSize(final SerializableFunction<Place, Long> size) throws Throwable {
		try {
			placeGroup.broadcastFlat(()-> {
				long expected = size.apply(here()) ;
				try {
					assertEquals(expected, distCol.size());	
				} catch(Throwable e) {
					RuntimeException re = new RuntimeException("Error on " + here());
					re.initCause(e);
					throw re;
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

	private void x_checkShift(final long expectedShift) throws Throwable {
		try {
			placeGroup.broadcastFlat(()-> {
				final long shift = (expectedShift + here().id) %NPLACES;
				try {
					// Check that each key/pair is on the right place
					for (LongRange lr : distCol.getAllRanges()) {
						long chunkNumber = lr.from / (rangeSize + rangeSkip);
						long apparentShift = (chunkNumber % NPLACES);
						assertEquals(shift, apparentShift);
					}} catch(Throwable e) {
						RuntimeException re = new RuntimeException("Error on " + here());
						re.initCause(e);
						throw re;
					}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

	@SuppressWarnings("deprecation")
	private void z_moveToPlaceZero() {
		placeGroup.broadcastFlat(() -> {
			try {
				MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
				Place destination = placeGroup.get(0);
				distCol.forEachChunk((RangedList<String> c) -> {
					ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
					cs.add(c);
					// System.out.println("[" + r.from + ".." + r.to + ") to " + destination.id);
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
	}

	@SuppressWarnings("deprecation")
	private void z_moveToNextPlace() {
		placeGroup.broadcastFlat(() -> {

			try {
				MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
				int rank = placeGroup.rank(here());
				Place destination = placeGroup.get(rank + 1 == placeGroup.size() ? 0 : rank + 1);
				distCol.forEachChunk((RangedList<String> c) -> {
					ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
					cs.add(c);
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
	}

	private void z_updateDist() {
		placeGroup.broadcastFlat(() -> {
			try {
				distCol.team().updateDist();
			} catch (Exception e) {
				System.err.println("Error on " + here());
				e.printStackTrace();
				throw e;
			}
		});
	}

	@SuppressWarnings("deprecation")
	private void z_distributeChunks() throws Throwable {		
		try {
			placeGroup.broadcastFlat(() -> {

				try {
					MoveManagerLocal mm = new MoveManagerLocal(placeGroup);
					distCol.forEachChunk((RangedList<String> c) -> {
						LongRange r = c.getRange();
						String s = c.get(r.from);
						// Every other chunk is sent to place 0 / 1
						int destination = (Integer.parseInt(s.split("/")[0])) % NPLACES;
						ArrayList<RangedList<String> > cs = new ArrayList<RangedList<String> >();
						cs.add(c);
						try {
							distCol.moveAtSync(cs, placeGroup.get(destination), mm);
						} catch (Exception e) {
							System.err.println("Error on " + here());
							e.printStackTrace();
						}
					});
					mm.sync();
				} catch (Exception e) {
					System.err.println("Error on " + here());
					e.printStackTrace();
					throw e;
				}
			});
		} catch (MultipleException me) {
			throw me.getSuppressed()[0];
		}
	}
}
