package handist.collections.dist;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.LongRange;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistRangedMap implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -6870647874233619233L;

    /** Number of entries that will move at per place */
    private static final long numDataToMove = 20;
    /***/
    private static final LongRange[] ranges = { new LongRange(0, 50), new LongRange(50, 100), new LongRange(100, 150),
            new LongRange(150, 200) };

    /** Random instance used to populate the map */
    private static Random random = new Random(12345);

    /**
     * Helper method to generate strings with the specified prefix
     *
     * @param prefix prefix of the random string returned
     * @return a random string with the specified prefix
     */
    public static String genRandStr(String prefix) {
        final long rand = random.nextLong();
        return prefix + rand;
    }

    /** distRangedMap which is the object of the tests of this class */
    DistRangedMap<Element> distRangedMap;

    /** PlaceGroup on which the distributed id map is defined */
    TeamedPlaceGroup pg;

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
    public void setup() throws Throwable {
        try {
            pg = TeamedPlaceGroup.getWorld();
            distRangedMap = new DistRangedMap<>(pg);
            pg.broadcastFlat(() -> {
                final int here = pg.rank();
                final LongRange r = ranges[here];
                distRangedMap.addRange(r);
                for (long i = r.from; i < r.to; i++) {
                    distRangedMap.put(i, new Element(genRandStr(here + "p")));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    private void testMoveNext(LongRange sendRange, LongRange receiveRange) throws Exception {
        final CollectiveMoveManager cmm = new CollectiveMoveManager(pg);
        final int here = pg.rank();
        final int next = (here + 1) % pg.size();

        distRangedMap.moveRangeAtSync(sendRange, pg.get(next), cmm);
        cmm.sync();
        distRangedMap.updateDist();

        assertEquals(pg.get(here), distRangedMap.getDistribution().location(receiveRange));
        assertEquals(pg.get(next), distRangedMap.getDistribution().location(sendRange));
        for (final Long l : receiveRange) {
            assertTrue(distRangedMap.containsKey(l));
        }
        for (final Long l : sendRange) {
            assertFalse(distRangedMap.containsKey(l));
        }
    }

    @Test
    public void testMoveRangeAtSyncFirstHalf() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final int here = pg.rank();
                final int prev = (pg.size() + here - 1) % pg.size();
                final LongRange sendRange = new LongRange(ranges[here].from, ranges[here].from + numDataToMove);
                final LongRange receiveRange = new LongRange(ranges[prev].from, ranges[prev].from + numDataToMove);
                final LongRange remainRange = new LongRange(ranges[here].from + numDataToMove, ranges[here].to);

                testMoveNext(sendRange, receiveRange);
                assertEquals(pg.get(here), distRangedMap.getDistribution().location(remainRange));
                for (final Long l : remainRange) {
                    assertTrue(distRangedMap.containsKey(l));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test
    public void testMoveRangeAtSyncLaterHalf() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final int here = pg.rank();
                final int prev = (pg.size() + here - 1) % pg.size();
                final LongRange sendRange = new LongRange(ranges[here].from + numDataToMove, ranges[here].to);
                final LongRange receiveRange = new LongRange(ranges[prev].from + numDataToMove, ranges[prev].to);
                final LongRange remainRange = new LongRange(ranges[here].from, ranges[here].from + numDataToMove);

                testMoveNext(sendRange, receiveRange);
                assertEquals(pg.get(here), distRangedMap.getDistribution().location(remainRange));
                for (final Long l : remainRange) {
                    assertTrue(distRangedMap.containsKey(l));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test
    public void testMoveRangeAtSyncMiddle() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final int here = pg.rank();
                final int prev = (pg.size() + here - 1) % pg.size();
                final LongRange sendRange = new LongRange(ranges[here].from + 5, ranges[here].to - 5);
                final LongRange receiveRange = new LongRange(ranges[prev].from + 5, ranges[prev].to - 5);
                final LongRange remainRange1 = new LongRange(ranges[here].from, ranges[here].from + 5);
                final LongRange remainRange2 = new LongRange(ranges[here].to - 5, ranges[here].to);

                testMoveNext(sendRange, receiveRange);
                assertEquals(pg.get(here), distRangedMap.getDistribution().location(remainRange1));
                assertEquals(pg.get(here), distRangedMap.getDistribution().location(remainRange2));
                for (final Long l : remainRange1) {
                    assertTrue(distRangedMap.containsKey(l));
                }
                for (final Long l : remainRange2) {
                    assertTrue(distRangedMap.containsKey(l));
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    @Test
    public void testUpdateDist() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                distRangedMap.updateDist();
                final Map<LongRange, Place> dist = distRangedMap.getDistribution().getDistribution();
                assertEquals(ranges.length, dist.size());
                for (int i = 0; i < dist.size(); i++) {
                    final Place pl = dist.get(ranges[i]);
                    assertEquals(pg.get(i), pl);
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }
}
