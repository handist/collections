package handist.collections.dist;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_DistSortedMap implements Serializable {

    /**
     * Static members and constants. These are either final or initialized in method
     * {@link #setUpBeforeClass()}.
     */
    /** Size of the dataset used for the tests **/
    public static final long numData = 200;
    /** Random object used to generate values */
    static Random random = new Random(12345l);
    /** Serial Version UID */
    private static final long serialVersionUID = 1L;

    /**
     * Helper method to generate Strings with the provided prefix.
     *
     * @param prefix the String prefix of the Random string generated
     * @return a random String with the provided prefix
     */
    public static String genRandStr(String prefix) {
        final long rndLong = random.nextLong();
        return prefix + rndLong;
    }

    /**
     * {@link DistMap} instance under test. Before each test, it is re-initialized
     * with {@value #numData} entries placed into it on host 0 and kept empty on
     * other hosts.
     *
     * @see #setUp()
     */
    DistSortedMap<Long, Element> distSortedMap;

    /** PlaceGroup object representing the collaboration between processes */
    TeamedPlaceGroup placeGroup;

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))
                && DebugFinish.suppressedExceptionsPresent()) {
            System.err.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        distSortedMap = new DistSortedMap<>(placeGroup);

        // Put some initial values in distSortedMap
        for (long l = 0; l < numData; l++) {
            distSortedMap.put(l, new Element(genRandStr("v")));
        }
    }

    @Test(timeout = 5000)
    public void testMoveAtSyncFromTo() throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                distSortedMap.moveAtSync(numData / 2, numData, placeGroup.get(1), mm);
                mm.sync();
                assertEquals(numData / 2, distSortedMap.size());
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test(timeout = 5000)
    public void testMoveAtSyncWithDistributionFromTo() throws Throwable {
        try {
            placeGroup.broadcastFlat(() -> {
                final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
                final Distribution<Long> dist = ((Long key) -> {
                    if (key < numData / 2) {
                        return placeGroup.get(0);
                    } else {
                        return placeGroup.get(1);
                    }
                });
                distSortedMap.moveAtSync(0l, numData, dist, mm);
                mm.sync();
                assertEquals(numData / 2, distSortedMap.size());
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }
}
