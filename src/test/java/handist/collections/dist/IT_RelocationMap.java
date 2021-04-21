package handist.collections.dist;

import static org.junit.Assert.*;

import apgas.MultipleException;

import java.io.Serializable;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_RelocationMap implements Serializable {

    /** Number of places this test is running on */
    static int NPLACES;

    /** Number of initial data entries places into the map */
    final static long numData = 200;

    /** Random instance used to populate the map with initial data */
    final static Random random = new Random(12345l);

    /**
     * Helper method used to generate strings with the specified prefix
     *
     * @param prefix prefix of the returned string
     * @return a randomly generated string which starts with the specified string
     */
    public static String genRandStr(String prefix) {
        final long rand = random.nextLong();
        return prefix + rand;
    }

    private RelocationMap<String, String> relocationMap;

    /** PlaceGroup on which the DistMap is defined on */
    TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();

    final Distribution<String> gatherDist = (key) -> {
        return pg.get(0);
    };

    @Before
    public void setup() {
        NPLACES = pg.size();
        relocationMap = new RelocationMap<>(gatherDist, pg);
    }

    @After
    public void tearDown() {
        relocationMap.destroy();
    }

    @Test
    public void testRelocate() {
	try {
	    pg.broadcastFlat(() -> {
		final int rank = pg.rank();
		for (int i = 0; i < numData; i++) {
			relocationMap.put(genRandStr("k" + rank), genRandStr("v" + rank));
		}
		relocationMap.relocate();

		if (rank == 0) {
			assertEquals(numData * NPLACES, relocationMap.size());
		} else {
			assertEquals(0, relocationMap.size());
		}
	    });
	} catch (MultipleException me) {
		me.printStackTrace();
	}
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testRelocateGlobal() {
        final Distribution distribution = (key) -> {
            return pg.get(1);
        };
        relocationMap.setDistribution(distribution);
	for (int i = 0; i < numData; i++) {
	    relocationMap.put(genRandStr("k"), genRandStr("v"));
	}
	try {
	    relocationMap.relocateGlobal();
	} catch (MultipleException me) {
	    me.printStackTrace();
	}
	pg.broadcastFlat(() -> {
	    final int rank = pg.rank();
	    if (rank == 1) {
		assertEquals(numData, relocationMap.size());
	    }
	});
    }
}
