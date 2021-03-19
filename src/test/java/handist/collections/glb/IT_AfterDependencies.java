package handist.collections.glb;

import static apgas.Constructs.*;
import static handist.collections.glb.GlobalLoadBalancer.*;
import static handist.collections.glb.Util.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.CollectiveMoveManager;
import handist.collections.dist.DistCol;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Class used to test various cases of After dependencies in GLB programs
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_AfterDependencies implements Serializable {

    /** Generated Serial Version UID */
    private static final long serialVersionUID = 3288118918209259718L;

    /** Number of ranges to populate this collection */
    final static long LONGRANGE_COUNT = 20l;

    /** Number of hosts on which this tests runs */
    final static int PLACEGROUP_SIZE = 4;

    /**
     * Size of individual ranges. This is made purposely an odd number such that
     * distributing the ranges can be done easily with a simple modulus operation.
     */
    final static long RANGE_SIZE = 100 * PLACEGROUP_SIZE + 1;
    /** Total number of elements contained in the {@link DistCol} */
    final static long TOTAL_DATA_SIZE = LONGRANGE_COUNT * RANGE_SIZE;

    private static void y_makeEvenDistribution(DistCol<Element> col) {
        col.placeGroup().broadcastFlat(() -> {
            final CollectiveMoveManager mm = new CollectiveMoveManager(col.placeGroup());
            col.forEachChunk((RangedList<Element> c) -> {
                final LongRange r = c.getRange();
                // We place the chunks everywhere except on place 3
                final Place destination = place((int) r.from % (PLACEGROUP_SIZE));
                col.moveRangeAtSync(r, destination, mm);
            });
            mm.sync();
        });
    }

    /**
     * Helper method which spreads the chunks contained in the collection given as
     * parameter evenly between places 0, 1, and 2. No instances are relocated to
     * place 3.
     *
     * @param col the collection to non-evenly distribute
     */
    private static void y_makePoorDistribution(DistCol<Element> col) {
        col.placeGroup().broadcastFlat(() -> {
            final CollectiveMoveManager mm = new CollectiveMoveManager(col.placeGroup());
            col.forEachChunk((RangedList<Element> c) -> {
                final LongRange r = c.getRange();
                // We place the chunks everywhere except on place 3
                final Place destination = place((int) r.from % (PLACEGROUP_SIZE - 1));
                col.moveRangeAtSync(r, destination, mm);
            });
            mm.sync();
        });
    }

    /**
     * Helper method which populates the collection given as parameter. The given
     * collection is assumed to be empty.
     *
     * @param col the collection to populate
     */
    private static void y_populateCollection(DistCol<Element> col, long rangeCount) {
        // Put some initial values in col
        for (long l = 0l; l < rangeCount; l++) {
            final long from = l * RANGE_SIZE;
            final long to = from + RANGE_SIZE;
            final String lrPrefix = "LR[" + from + ";" + to + "]";
            final LongRange lr = new LongRange(from, to);
            final Chunk<Element> c = new Chunk<>(lr);
            for (long i = from; i < to; i++) {
                final String value = genRandStr(lrPrefix + ":" + i + "#");
                c.set(i, new Element(value));
            }
            col.add(c);
        }
    }

    /**
     * Checks that the distCol contains exactly the specified number of entries. The
     * {@link DistCol#size()} needs to match the specified parameter.
     *
     * @param col           DistCol whose global size is to be checked
     * @param expectedCount expected total number of entries in the DistCol instance
     * @throws Throwable if thrown during the check
     */
    private static void z_checkDistColTotalElements(DistCol<Element> col, long expectedCount) throws Throwable {
        long count = 0;
        for (final Place p : col.placeGroup().places()) {
            count += at(p, () -> {
                return col.size();
            });
        }
        assertEquals(expectedCount, count);
    }

    /** DistCol used to test the GLB functionalities */
    DistCol<Element> collection1, collection2;

    /** PlaceGroup on which collection #col is defined */
    TeamedPlaceGroup placeGroup;

    @Before
    public void setup() {
        placeGroup = TeamedPlaceGroup.getWorld();
        collection1 = new DistCol<>(placeGroup);
        collection2 = new DistCol<>(placeGroup);

        y_populateCollection(collection1, LONGRANGE_COUNT);
        y_makePoorDistribution(collection1);

        y_populateCollection(collection2, LONGRANGE_COUNT / 2);
        y_makeEvenDistribution(collection2);
    }

    @After
    public void tearDown() {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            GlbComputer.destroyGlbComputer();
        });
        collection1.destroy();
        collection2.destroy();
    }

    /**
     * This checks that the second operation only starts after the first one has
     * completed. If the contrary occurs, assertions will fail.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testAfter() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            final DistFuture<DistCol<Element>> test = collection1.GLB.forEach(makePrefixTest);
            collection1.GLB.forEach((e) -> assertTrue(e.s.startsWith("Test"))).after(test);
        });
        if (!ex.isEmpty()) {
            throw ex.get(0);
        }
        z_checkDistColTotalElements(collection1, TOTAL_DATA_SIZE);
    }

    @Test(timeout = 10000)
    public void testAfterOnCompletedOperation() throws Throwable {
        final ArrayList<Exception> exceptions = underGLB(() -> {
            final DistFuture<DistCol<Element>> future = collection1.GLB.forEach(makePrefixTest);
            future.waitGlobalTermination();
            collection1.GLB.forEach(makeSuffixTest).after(future);
        });
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        z_checkDistColTotalElements(collection1, TOTAL_DATA_SIZE);
    }

    @Test(timeout = 10000)
    public void testAfterOnRunningOperation() throws Throwable {
        final ArrayList<Exception> exceptions = underGLB(() -> {
            final DistFuture<DistCol<Element>> earlyFuture = collection2.GLB.forEach(makePrefixTest);
            final DistFuture<DistCol<Element>> laterFuture = collection1.GLB.forEach(makePrefixTest);
            earlyFuture.waitGlobalTermination();
            final List<Throwable> errors = collection1.GLB.forEach(e -> {
                assertTrue(e.s.startsWith("Test"));
            }).after(laterFuture).getErrors();
            if (!errors.isEmpty()) {
                System.err.println("There were " + errors.size() + " errors in testAfterOnRunningOperation");
                throw new RuntimeException(errors.get(0)); // Pack the first error as the cause of the RuntimeException
            }
        });
        if (!exceptions.isEmpty()) {
            throw exceptions.get(0);
        }
        z_checkDistColTotalElements(collection1, TOTAL_DATA_SIZE);
        z_checkDistColTotalElements(collection2, TOTAL_DATA_SIZE / 2);
    }

    /**
     * This checks that the last operation only starts after the first two
     * completed. If the contrary occurs, assertions will fail.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testAfterTwo() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            //
            final DistFuture<DistCol<Element>> test = collection1.GLB.forEach(makePrefixTest);
            final DistFuture<DistCol<Element>> addZ = collection1.GLB.forEach(addZToPrefix).after(test);
            collection1.GLB.forEach((e) -> assertTrue(e.s.startsWith("ZTest"))).after(addZ);
        });
        if (!ex.isEmpty()) {
            throw ex.get(0);
        }
        z_checkDistColTotalElements(collection1, TOTAL_DATA_SIZE);
    }

    /**
     * Same test as {@link #testAfterTwo()}, but using an alternative writing.
     * <p>
     * Here, the dependency of the third operation on the completion of the first
     * two operations is programmed explicitly. In practice this is redundant since
     * the second operation also depends on the completion of the first operation,
     * but it is better to explicitly check.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testAfterTwo_Alternative() throws Throwable {
        final ArrayList<Exception> ex = underGLB(() -> {
            final DistFuture<DistCol<Element>> test = collection1.GLB.forEach(makePrefixTest);
            final DistFuture<DistCol<Element>> addZ = collection1.GLB.forEach(addZToPrefix).after(test);
            collection1.GLB.forEach((e) -> assertTrue(e.s.startsWith("ZTest"))).after(test).after(addZ);
        });
        if (!ex.isEmpty()) {
            for (final Exception e : ex) {
                e.printStackTrace();
            }
            throw ex.get(0);
        }
        z_checkDistColTotalElements(collection1, TOTAL_DATA_SIZE);
    }

    /**
     * Checks that the correct number of elements was created as well as the
     * distribution of these instances
     */
    @Test(timeout = 10000)
    public void testSetup() throws Throwable {
        z_checkDistColTotalElements(collection1, TOTAL_DATA_SIZE);
        z_checkDistColTotalElements(collection2, TOTAL_DATA_SIZE / 2);
    }
}