package handist.collections.glb.lifeline;

import static handist.collections.glb.lifeline.LifelineTestHelper.*;

import java.io.Serializable;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_LifelineTester implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 1141787756344994164L;

    /**
     * Place group instances used for the tests
     */
    static TeamedPlaceGroup WORLD; // TODO add some other configurations, places012, places023, places123,
                                   // places23;

    /**
     * All place groups placed in an array for convenience
     */
    static TeamedPlaceGroup[] allPG;

    /**
     * Prepares place groups and lifeline implementations to test
     *
     * @throws Exception if thrown during setup
     */
    @BeforeClass
    public static void setupBefore() throws Exception {
        WORLD = TeamedPlaceGroup.getWorld();

        allPG = new TeamedPlaceGroup[1];
        allPG[0] = WORLD;
    }

    @Test
    public void testHypercube() {
        for (final TeamedPlaceGroup pg : allPG) {
            final Hypercube hypercubeLifeline = new Hypercube(pg);
            checkLifeline(hypercubeLifeline);
            checkReverseLifeline(hypercubeLifeline);
        }
    }

    @Test
    public void testLoop() throws Throwable {
        for (final TeamedPlaceGroup pg : allPG) {
            final Loop loopLifeline = new Loop(pg);
            checkLifeline(loopLifeline);
            checkReverseLifeline(loopLifeline);
        }
    }
}
