package handist.collections.dist;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 3, launcher = TestLauncher.class)
public class IT_DistLog implements Serializable {

    private static final long serialVersionUID = -1479339859576363444L;
    /** Number of places this test is running on */
    static int NPLACES;

    final static long numSteps = 3;

    /** Number of initial data entries places into the map */
    final static long numData = 5;

    /** PlaceGroup on which the DistMap is defined on */
    TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))) {
            System.out.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    @Test(timeout = 10000)
    public void run() throws Throwable {
        try {
            assertNotNull(DistLog.map);
            assertNull(DistLog.defaultLog);
            DistLog.log("beforeSetup", "message before setup", System.currentTimeMillis());
            assertNotNull(pg);
            DistLog.globalSetup(pg, 0, true);
            assertNotNull(DistLog.defaultLog);
            pg.broadcastFlat(() -> {
                assertNotNull(DistLog.defaultLog);
                assertTrue(DistLog.map.size() == 1);
            });
            final DistLog dlog1 = DistLog.globalSetup(pg, 0, false);
            final DistLog dlog2 = DistLog.globalSetup(pg, 0, false);

            for (int step = 0; step < numSteps; step++) {
                pg.broadcastFlat(() -> {
                    finish(() -> {
                        for (int i = 0; i < numData; i++) {
                            final String tag = "tag" + i;
                            async(() -> {
                                DistLog.log(tag, "msg0", "appendix:" + System.currentTimeMillis());
                            });
                        }
                        for (int i = 0; i < numData; i++) {
                            final String tag = "tag" + i;
                            dlog1.put(tag, "msg0", "time:" + System.currentTimeMillis());
                        }
                    });
                });
                for (int p = 0; p < NPLACES; p++) {
                    for (int i = 0; i < numData; i++) {
                        final String tag = "tag" + i;
                        dlog2.put(tag, "msg0", "time:" + System.currentTimeMillis());
                    }
                }
                DistLog.defaultGlobalSetPhase(step);
                dlog1.globalSetPhase(step);
                dlog2.globalSetPhase(step);
            }
            DistLog.defaultGlobalGather();
            dlog1.globalGather();

            DistLog.defaultLog.printAll(System.out);
            System.out.println("This is a part of test");
            dlog1.printAll(System.out);
            System.out.println("This is a part of test, dlog2");
            dlog2.printAll(System.out);

            assertTrue(dlog1.placeConsciousEquals(DistLog.defaultLog, System.out, true));
            assertTrue(dlog1.placeConsciousEquals(DistLog.defaultLog, System.out, false));

            assertFalse(dlog1.placeConsciousEquals(dlog2, System.out, false));
            assertTrue(dlog1.distributionFreeEquals(dlog2, System.out));
        } catch (final Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Before
    public void setup() {
        NPLACES = pg.size();
    }

}
