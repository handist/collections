package handist.collections.dist;

import static org.junit.Assert.*;
import static apgas.Constructs.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=4, launcher=TestLauncher.class)
public class IT_DistCol2 implements Serializable {
	
	/** Serial Version UID */
	private static final long serialVersionUID = -8191119224439969307L;

	static Random random;

	/** TeamedPlaceGroup representing the whole world */
	TeamedPlaceGroup world;

	LongRange firstRange, secondRange, thirdRange;

	Chunk<Element> firstChunk, secondChunk, thirdChunk;

	/** Instance used under test */
	DistCol<Element> distCol;

	/**
	 * Helper method which fills the given chunk with random values
	 * @param c the chunk to fill with values
	 */
	static void fillWithValues(Chunk<Element> c, String prefix) {
		c.forEach((index,s)->{
			Element e = new Element(prefix + random.nextInt(1000));
			c.set(index, e);
			//			c.set(index, new IT_DistCol2.Element(prefix + random.nextInt(1000)));
		});
	}

	@BeforeClass
	public static void setupBeforeClass() {
		random = new Random(12345l);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		world = TeamedPlaceGroup.getWorld();

		firstRange = new LongRange(0l, 100l);
		secondRange = new LongRange(100l, 200l);
		thirdRange = new LongRange(200l, 250l);

		firstChunk = new Chunk<>(firstRange);
		secondChunk = new Chunk<>(secondRange);
		thirdChunk = new Chunk<>(thirdRange);

		fillWithValues(firstChunk, "a");
		fillWithValues(secondChunk, "b");
		fillWithValues(thirdChunk, "c");

		distCol = new DistCol<>(world);
	}

	@After
	public void tearDown() throws Exception {
		distCol.destroy();
	}

	/** 
	 * Checks that the distCol is in the expected state 
	 * after method {@link #setUp()} is called 
	 */
	@Test
	public void testSetup() {
		world.broadcastFlat(()->{
			assertTrue(distCol.isEmpty());
			assertEquals(0l, distCol.size());
		});
	}

	/**
	 * Checks that adding a chunk in a place makes remote handles aware of the change
	 * @throws Throwable if such a throwable is thrown during the test
	 */
	@Test(timeout=10000)
	public void testAddUpdatesDistributedInformation() throws Throwable {
		distCol.add(firstChunk); // Add a chunk to local handle of place 0 
		distCol.placeGroup().broadcastFlat(()->{
			long[] size = new long[world.size()]; 

			distCol.TEAM.updateDist();	// Here is the important call
			distCol.TEAM.size(size);	// We check the result of TEAM.size

			assertEquals(world.size(), size.length);
			for (int i=0; i<size.length; i++) {
				long expectedSize = i==0? 100l: 0l;
				assertEquals(expectedSize, size[i]);
			}
		});
	}

	/**
	 * Checks that the global forEach operates as intended
	 * @throws Throwable if such a throwable is thrown during the test
	 */
	@Test(timeout=10000)
	public void testGlobalForEach() throws Throwable {
		try {
			// Place chunks in different handles
			distCol.placeGroup().broadcastFlat(()->{
				switch(here().id) {
				case 0:
					distCol.add(firstChunk);
					break;
				case 1:
					distCol.add(secondChunk);
					break;
				case 2:
					distCol.add(thirdChunk);
					break;
				}
			});

			// Call GLOBAL forEach
			distCol.GLOBAL.forEach((e)->{
				e.s = "testGlobal" + e.s;
			});

			// Check that every string was modified

			distCol.placeGroup().broadcastFlat(()->{
				for (Element e : distCol) {
				    assertTrue(e.s.startsWith("testGlobal"));
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

    	/**
	 * Checks that the global forEach operates as intended
	 * @throws Throwable if such a throwable is thrown during the test
	 */
	@Test(timeout=10000)
	public void testAlternativeGlobalForEach() throws Throwable {
		try {
			// Place chunks in different handles
			distCol.placeGroup().broadcastFlat(()->{
				switch(here().id) {
				case 0:
					distCol.add(firstChunk);
					break;
				case 1:
					distCol.add(secondChunk);
					break;
				case 2:
					distCol.add(thirdChunk);
					break;
				}
			});

			// Make a manual forEach
			// distCol.GLOBAL.forEach((s)->{
			// 	System.out.println(s); // = "testGlobal" + s;
			// });

			distCol.placeGroup().broadcastFlat(()->{
				distCol.forEach((e)->{
					e.s = "testGlobal" + e.s;
				});
			});

			// Check that every string was modified

			distCol.placeGroup().broadcastFlat(()->{
				for (Element e : distCol) {
				    assertTrue(e.s.startsWith("testGlobal"));
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}

}
