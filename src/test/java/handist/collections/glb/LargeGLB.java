package handist.collections.glb;

import static apgas.Constructs.*;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.glb.lifeline.Loop;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test program which uses a larger dataset to be able to test load balancing
 * features.
 *
 * @author Patrick Finnerty
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class LargeGLB implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 5734175886780421274L;

    /**
     * Place group that contains all the hosts in the computation
     */
    static TeamedPlaceGroup WORLD;

    /**
     * Number of chunks present in {@link #col}
     */
    static final int CHUNK_COUNT = 10000;

    /**
     * Size of each individual chunk in {@link #col}
     */
    static final int CHUNK_SIZE = 500;

    /**
     * In this setup method, some properties are set to influence the behavior of
     * the GLB
     *
     * @throws Exception if thrown during setup
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            WORLD = TeamedPlaceGroup.getWorld();
            // System.setProperty(Config.ACTIVATE_TRACE, "true");
            System.setProperty(Config.GRANULARITY, "5"); // Exceptionally low
            System.setProperty(Config.LIFELINE_STRATEGY, Loop.class.getCanonicalName());
            System.setProperty(Config.MAXIMUM_WORKER_COUNT, "2"); // Set low
        });
    }

    DistCol<Integer> col;

    @Before
    public void setUp() throws Exception {
        col = new DistCol<>(WORLD);
        z_populateCollection(col);
    }

    @After
    public void tearDown() throws Exception {
        col.destroy();
    }

    @Test(timeout = 60000)
    public void test() {
        GlobalLoadBalancer.underGLB(() -> {
            col.GLB.forEach(i -> {
                // This forEach contains some dummy operation to simulate a certain load
                final byte[] hashArray = new byte[20];
                final Random r = new Random(i);
                r.nextBytes(hashArray);
                MessageDigest digest = null;
                try {
                    digest = MessageDigest.getInstance("SHA-1");
                } catch (final NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                digest.digest(hashArray);
            });
        });
    }

    /**
     * Helper method which populates the provided distributed collection
     *
     * @param collection collection to populate
     */
    private void z_populateCollection(DistCol<Integer> collection) {
        long rangeBegin = 0; // inclusive
        long rangeEnd; // exclusive
        try {
            for (long i = 0; i < CHUNK_COUNT; i++) {
                rangeEnd = rangeBegin + CHUNK_SIZE - 1;
                final Chunk<Integer> c = new Chunk<>(new LongRange(rangeBegin, rangeEnd), 0);
                for (long j = rangeBegin; j < rangeEnd; j++) {
                    c.set(j, (int) j);
                }
                collection.add(c);
                rangeBegin = rangeBegin + CHUNK_SIZE;
            }
        } catch (final Exception e) {
            System.err.println("Error on " + here());
            e.printStackTrace();
            throw e;
        }
    }

}
