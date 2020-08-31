package handist.collections.dist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=4, launcher=TestLauncher.class)
public class IT_DistIdMap2 implements Serializable {
	
	
	/** Serial Version UID */
	private static final long serialVersionUID = 7279840236820619500L;
	
	/** Random object used to generate values */
	static Random random = new Random(12345l);
	
	/** Instance under test */
	DistIdMap<Element> distIdMap;
	
	/** World on which the test is running */
	static TeamedPlaceGroup WORLD = TeamedPlaceGroup.getWorld();
	
	/** Number of entries placed into the distIdMap per place */
	long ENTRIES_PER_PLACE = 100;
	
	private static String genRandStr(String prefix) {
		long rndLong = random.nextLong();
		return prefix + rndLong;
	}
	
	@Before
	public void setUp() throws Throwable {
		distIdMap = new DistIdMap<>();
		WORLD.broadcastFlat(()->{
			int here = WORLD.myRank();
			for (long index = here * ENTRIES_PER_PLACE; index < (here +1) * ENTRIES_PER_PLACE; index++) {
				distIdMap.put(index, new Element(genRandStr(here + "p")));				
			}
		});
	}
	
	@After
	public void tearDown() throws Throwable {
		distIdMap.destroy();
	}
	
	@Test(timeout = 5000)
	public void testGlobalForEach() throws Throwable {
		distIdMap.GLOBAL.forEach((e)->{
			e.s = "testGlobal" + e.s;
		});
		
		try {
			WORLD.broadcastFlat(()->{
				for (Element e : distIdMap.values()) {
				    assertTrue(e.s.startsWith("testGlobal"));
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}
	
	@Test(timeout = 5000)
	public void testSetUp() throws Throwable {
		try {
			WORLD.broadcastFlat(()->{
				// Check that the correct nb of entries have benn initialized
				assertEquals(ENTRIES_PER_PLACE, distIdMap.size());
				
				// Check all local elements have the correct prefix
				int here = WORLD.myRank();
				for (Element e: distIdMap.values()) {
					assertTrue(e.s.startsWith(here + "p"));
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}
}
