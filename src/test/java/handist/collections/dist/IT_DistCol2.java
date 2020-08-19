package handist.collections.dist;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
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

	Chunk<String> firstChunk, secondChunk, thirdChunk;

	/** Instance used under test */
	DistCol<String> distCol;

	/**
	 * Helper method which fills the given chunk with random values
	 * @param c the chunk to fill with values
	 * @param the prefix of every random value set in the chunk
	 */
	static void fillWithValues(Chunk<String> c, String prefix) {
		c.forEach((index,s)->{
			c.set(index, prefix + random.nextInt(1000));
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
	@Test(timeout=20000)
	public void testAdd() throws Throwable {
		distCol.add(firstChunk);
		try {
			distCol.placeGroup().broadcastFlat(()->{
				long[] size = new long[world.size()]; 

				distCol.TEAM.updateDist();
				distCol.TEAM.size(size);

				assertEquals(world.size(), size.length);
				for (int i=0; i<size.length; i++) {
					long expectedSize = i==0? 100l: 0l;
					assertEquals(expectedSize, size[i]);
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0].fillInStackTrace();
		}
	}
}
