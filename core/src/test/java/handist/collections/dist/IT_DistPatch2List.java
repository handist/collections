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

import apgas.Place;
import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.patch.Element;
import handist.collections.patch.Patch2D;
import handist.collections.patch.Range2D;
import handist.collections.patch.Position2D;
import handist.collections.patch.Index2D;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistPatch2List implements Serializable {

    /** the number of elements each patch has */
    private static final int numData = 5;
    private static final int seed = 12345;
    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    private final Range2D range = new Range2D(new Position2D(0, 0), new Position2D(8, 4));
    private final int xSplit = 4;
    private final int ySplit = 4;
    private Position2D gridSize;

    private final TeamedPlaceGroup placeGroup = TeamedPlaceGroup.getWorld();
    private DistPatch2DList<Element> distPatchList;

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
    public void setup() {
        distPatchList = new DistPatch2DList<>(new Patch2D<>(range), xSplit, ySplit, placeGroup);
        gridSize = new Position2D(range.size().x / xSplit, range.size().y / ySplit);

        final Random rand = new Random(seed);
        distPatchList.forEachPatch((patch) -> {
            for (int i = 0; i < numData; i++) {
                final double x = patch.getRange().leftBottom.x + rand.nextDouble() * gridSize.x;
                final double y = patch.getRange().leftBottom.y + rand.nextDouble() * gridSize.y;
                patch.put(new Element(x, y, distPatchList.getIndex(patch)));
            }
        });
    }

    @Test(timeout = 5000)
    public void testGetSizeDistribution() {
        testMove();
        placeGroup.broadcastFlat(() -> {
            final long[] result = new long[placeGroup.size()];
            distPatchList.updateDist();
            distPatchList.getSizeDistribution(result);
            final long expected = xSplit * ySplit / placeGroup.size();
            for (int i = 0; i < result.length; i++) {
                assertEquals(expected, result[i]);
            }
        });
    }

    @Test(timeout = 10000)
    public void testMigrate() {
        testMove();
        placeGroup.broadcastFlat(() -> {
            // each patch migrates one element to the upper patch.
            distPatchList.forEachPatch((patch) -> {
                final Element elem = patch.iterator().next();
                // update position.y. border cyclic
                final double y = (elem.position().y + gridSize.y) % range.rightTop.y;
                elem.move(new Position2D(elem.position().x, y));
            });
            // migrate
            distPatchList.migrate();
            // There should be only one element moved from the below patch
            distPatchList.forEachPatch((patch) -> {
                assertEquals(numData, patch.size());
                final Index2D index = distPatchList.getIndex(patch);
                final Index2D belowIndex = new Index2D(index.x, (index.y - 1 + ySplit) % ySplit);
                int count = 0;
                for (final Element e : patch) {
                    if (e.belongPatch.equals(belowIndex)) {
                        count++;
                    } else if (!e.belongPatch.equals(index)) {
                        throw new AssertionError("Patch " + patch + " receiv an unexpected element");
                    }
                }
                assertEquals(1, count);
            });
        });

    }

    private void testMove() {
        placeGroup.broadcastFlat(() -> {
            // Cyclic partitioning with y as the outer loop and x as the inner loop
            final Distribution<Index2D> rule = ((i) -> {
                return new Place((i.y * xSplit + i.x) % placeGroup.size());
            });
            final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup);
            distPatchList.moveAtSync(rule, mm);
            mm.sync();
        });
    }

}
