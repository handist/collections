package handist.collections.dist;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

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
public class IT_DistMultiMap2 implements Serializable{

	/** Serial Version UID */
	private static final long serialVersionUID = -4212799809586932921L;
	
	/** Random object used to generate values */
	static Random random = new Random(12345l);
	
	/** Number of mappings on each host */
	final private long NB_MAPPINGS = 20l;
	
	/** Maximum number of entries in each map */
	final private int MAX_NB_VALUES_PER_MAPPING = 50;
	
	final private TeamedPlaceGroup WORLD = TeamedPlaceGroup.getWorld();
	
	/** Instance under test */
	DistMultiMap<String, Element> distMultiMap;
	
	private static String genRandStr(String prefix) {
		long rndLong = random.nextLong();
		return prefix + rndLong;
	}
	
	@Before
	public void setUp() throws Throwable {
		distMultiMap = new DistMultiMap<>(WORLD);
		WORLD.broadcastFlat(()-> {
			int here = WORLD.myRank();
			for (long l = 0; l < NB_MAPPINGS; l++) {
				String key = genRandStr(here + "k");
				
				int nbMappings = 1 + random.nextInt(MAX_NB_VALUES_PER_MAPPING - 1);
				List<Element> values = new ArrayList<>(nbMappings);
				for (int v = 0; v < nbMappings; v++) {
					values.add(new Element(genRandStr(here + "v" + l + "m")));
				}
				distMultiMap.put(key, values);
			}
		});
	}
	
	@After
	public void tearDown() throws Throwable {
		distMultiMap.destroy();
	}
	
	@Test(timeout = 5000)
	public void testSetUp() throws Throwable {
		try {
			WORLD.broadcastFlat(()-> {
				int here = WORLD.myRank();
				Set<Map.Entry<String,List<Element>>> entrySet = distMultiMap.entrySet();
				assertEquals(entrySet.size(), NB_MAPPINGS);
				for (Map.Entry<String, List<Element>> entry : entrySet) {
					assertTrue(entry.getKey().startsWith(here + "k"));
					for (Element e : entry.getValue()) {
						assertTrue(e.s.startsWith(here + "v"));
					}
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}
	
	@Test(timeout = 5000)
	public void testGlobalForEach() throws Throwable {
		final String prefix = "TESTGLOBALFOREACH";
		// Add a prefix to all the first element of the lists
		distMultiMap.GLOBAL.forEach((l)->{
			Element firstElement = l.get(0);
			firstElement.s = prefix + firstElement.s;
		});
		
		//Check the prefix was added to all first mappings of each key
		try {
			WORLD.broadcastFlat(()->{
				for (List<Element> mappings : distMultiMap.values()) {
					// The first mapping has the prefix
					assertTrue(mappings.remove(0).s.startsWith(prefix));
					
					// The remaining mappings were left untouched
					for (Element e : mappings) {
						assertFalse(e.s.startsWith(prefix));
					}
				}
			});
		} catch (MultipleException me) {
			me.printStackTrace();
			throw me.getSuppressed()[0];
		}
	}
}
