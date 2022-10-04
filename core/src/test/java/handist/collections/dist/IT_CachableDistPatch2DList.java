package handist.collections.dist;

import static apgas.Constructs.*;
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

import apgas.Place;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.patch.Index2D;
import handist.collections.patch.Patch2D;
import handist.collections.patch.Position2D;
import handist.collections.patch.Positionable;
import handist.collections.patch.Range2D;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_CachableDistPatch2DList implements Serializable {

    private static class Element implements Positionable<Position2D>, Serializable {
        private static final long serialVersionUID = 4690589208580205315L;
        private final Position2D pos;
        public int value, patchId;

        public Element(double x, double y, int patchId) {
            this.pos = new Position2D(x, y);
            this.value = 0;
            this.patchId = patchId;
        }

        @Override
        public Position2D position() {
            return pos;
        }
    }

    private static final long serialVersionUID = -7632072644551475244L;

    private static final Range2D worldRange = new Range2D(new Position2D(0, 0), new Position2D(4.0, 4.0));
    private static final Index2D numPatch = new Index2D(4, 4);
    private static final int numPatchData = 4;

    private static final int seed = 12345;

    private static final Distribution<Index2D> ownerDist = (index) -> {
        return new Place(index.y);
    };
    private static final Distribution<Index2D> cacheDist = (index) -> {
        final int p0 = (index.y - 1 + numPatch.y) % numPatch.y;
        if (index.x == p0) {
            return new Place(p0);
        }
        final int p1 = (index.y + 1) % numPatch.y;
        if (index.x == p1) {
            return new Place(p1);
        }
        return null;
    };

    private CachableDistPatch2DList<Element> caPatchList;
    private TeamedPlaceGroup placeGroup;

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
    public void setUp() {
        placeGroup = TeamedPlaceGroup.getWorld();
        caPatchList = new CachableDistPatch2DList<>(new Patch2D<>(worldRange), numPatch.x, numPatch.y, placeGroup);
        final Random rand = new Random(seed);
        // initialize elements
        caPatchList.forEachPatch((patch) -> {
            for (int i = 0; i < numPatchData; i++) {
                final double x = patch.getRange().leftBottom.x + rand.nextDouble() * patch.getRange().size().x;
                final double y = patch.getRange().leftBottom.y + rand.nextDouble() * patch.getRange().size().y;
                patch.put(new Element(x, y, patch.id()));
            }
        });
        // move and cache
        placeGroup.broadcastFlat(() -> {
            final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
            caPatchList.moveAtSync(ownerDist, mm);
            mm.sync();

            caPatchList.forEachPatch((patch) -> {
                final Place dest = cacheDist.location(caPatchList.getIndex(patch));
                if (dest != null) {
                    caPatchList.sharePatchAtSync(patch, dest, mm);
                }
            });
            mm.sync();
        });
    }

    @Test(timeout = 5000)
    public void testForEachOwnPatch() {
        placeGroup.broadcastFlat(() -> {
            caPatchList.parallelForEachOwnPatch(2, (patch) -> {
                assertEquals(here().id, caPatchList.getIndex(patch).y);
            });
        });
    }

    @Test(timeout = 10000)
    public void testParallelForEachOwnPatch() {
        placeGroup.broadcastFlat(() -> {
            caPatchList.parallelForEachOwnPatch(2, (patch) -> {
                assertEquals(here().id, caPatchList.getIndex(patch).y);
                patch.forEach((i, elem) -> {
                    elem.value = 1;
                });
            });

            caPatchList.forEachPatch((patch) -> {
                if (caPatchList.isCached(patch)) {
                    patch.forEach((i, elem) -> {
                        assertEquals(0, elem.value);
                    });
                } else {
                    patch.forEach((i, elem) -> {
                        assertEquals(1, elem.value);
                    });
                }
            });
        });
    }

    @Test(timeout = 5000)
    public void testSetUp() {
        placeGroup.broadcastFlat(() -> {
            final int[] count = { 0, 0 };
            caPatchList.forEachPatch((patch) -> {
                if (caPatchList.isCached(patch)) {
                    count[0]++;
                    assertNotEquals(here(), ownerDist.location(caPatchList.getIndex(patch)));
                } else {
                    count[1]++;
                    assertEquals(here().id, caPatchList.getIndex(patch).y);
                }
            });
            assertEquals(2, count[0]);
            assertEquals(4, count[1]);
        });
    }
}
