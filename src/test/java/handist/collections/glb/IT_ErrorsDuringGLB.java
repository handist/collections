package handist.collections.glb;

import static apgas.Constructs.*;
import static handist.collections.glb.GlobalLoadBalancer.*;
import static handist.collections.glb.Util.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.hamcrest.MatcherAssert;
import org.hamcrest.core.IsInstanceOf;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.CollectiveMoveManager;
import handist.collections.dist.DistCol;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.glb.DistColGlb.DistColGlbError;
import handist.collections.glb.lifeline.NoLifeline;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_ErrorsDuringGLB implements Serializable {

    /** Number of ranges to populate this collection */
    final static long LONGRANGE_COUNT = 20l;

    /** Number of hosts on which this tests runs */
    final static int PLACEGROUP_SIZE = 4;

    /** Size of individual ranges */
    final static long RANGE_SIZE = 100 + 1;
    /** Serial Version UID */
    private static final long serialVersionUID = 3890454865986201964L;

    /** Total number of elements contained in the {@link DistCol} */
    final static long TOTAL_DATA_SIZE = LONGRANGE_COUNT * RANGE_SIZE;

    /**
     * Puts in place a number of setups useful to make the tests predictable in
     * nature. In particular for this test class, the "NoLifeline" lifeline strategy
     * is used to make sure that the errors occur on the place on which they are
     * distributed.
     */
    @BeforeClass
    public static void before() throws Exception {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            System.setProperty(Config.LIFELINE_STRATEGY, NoLifeline.class.getCanonicalName());
        });
    }

    private static <T> void y_makeDistribution(DistCol<T> col) {
        // Transfer elements to remote hosts (Places 0, 1, 2 - 3 doesn't get any)
        final TeamedPlaceGroup pg = col.placeGroup();
        pg.broadcastFlat(() -> {
            final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
            col.forEachChunk((RangedList<T> c) -> {
                final LongRange r = c.getRange();
                // We place the chunks everywhere except on place 3
                final Place destination = place((int) r.from % (PLACEGROUP_SIZE - 1));
                col.moveRangeAtSync(r, destination, mm);
            });
            mm.sync();
        });
    }

    /**
     * Helper method which fill the provided DistCol with values
     *
     * @param col the collection which needs to be populated
     */
    private static void y_populateDistCol(DistCol<Element> col) {
        for (long l = 0l; l < LONGRANGE_COUNT; l++) {
            final long from = l * RANGE_SIZE;
            final long to = from + RANGE_SIZE;
            final String lrPrefix = "LR[" + from + ";" + to + "]";
            final LongRange lr = new LongRange(from, to);
            final Chunk<Element> c = new Chunk<>(lr);
            for (long i = from; i < to; i++) {
                final String value = lrPrefix + ":" + i + "#";
                c.set(i, new Element(value));
            }
            col.add(c);
        }
    }

    /**
     * Distributed collection which is the object of the tests. It is defined on the
     * entire world.
     */
    DistCol<Element> distCol;

    /**
     * Whole world
     */
    TeamedPlaceGroup placeGroup;

    @Before
    public void setUp() throws Exception {
        placeGroup = TeamedPlaceGroup.getWorld();
        distCol = new DistCol<>();

        y_populateDistCol(distCol);
        y_makeDistribution(distCol);
    }

    @After
    public void tearDown() throws Exception {
        distCol.destroy();
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            GlbComputer.destroyGlbComputer();
        });
    }

    /**
     * Checks the behavior of the GLB when an exception is thrown inside of the
     * lambda expression supplied by the user.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testExceptionDuringGlbOperation() throws Throwable {
        // Put a NULL at a certain index
        final long nullIndex = 5;
        distCol.set(nullIndex, null);

        // Perform a forEach on all the elements
        // The GLB should apply the forEach on every element and keep an Exception for
        // the index "nullIndex"
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final List<Throwable> errors = distCol.GLB.forEach(makeSuffixTest).getErrors();
                // We should have 1 exception in the errors returned by the GLB operation
                assertEquals(1, errors.size());

                // We check that the exception we got is indeed what we expect it to be
                final Throwable t = errors.get(0);
                // T is supposed to be the wrapper "DistColGlbError", which indicates that the
                // problem occurred on index "nullIndex"
                MatcherAssert.assertThat(t, IsInstanceOf.instanceOf(DistColGlb.DistColGlbError.class));
                assertEquals(nullIndex, ((DistColGlbError) t).index);
                // The cause should be a NullPointerException
                MatcherAssert.assertThat(t.getCause(), IsInstanceOf.instanceOf(NullPointerException.class));

                // We check that the operation was indeed applied to all other indices
                final List<Throwable> checkErrors = distCol.GLB.forEach((l, e) -> {
                    if (l == nullIndex) {
                        // Check that the element is indeed null
                        assertNull(e);
                    } else {

                        // Check that the element has the correct prefix
                        assertTrue(e.s.endsWith("Test"));
                    }
                }).getErrors();

                // Potential assertion failures would be caught in the previous forEach
                // operation's errors.
                assertTrue("There were <" + checkErrors.size() + "> errors when we expected <0>",
                        checkErrors.isEmpty());
            });
            assertEquals(0, ex.size()); // There shouldn't be any error thrown from inside the GLB
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
    }

    /**
     * Checks the behavior of the GLB when an exception is thrown inside of the
     * lambda expression supplied by the user.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 10000)
    public void testExceptionDuringGlbOperationOnRemoteHost() throws Throwable {
        // Put a NULL at a certain index
        final long nullIndex = 150;
        at(place(2), () -> {
            distCol.set(nullIndex, null); // the exception will occur on place 2
        });

        // Perform a forEach on all the elements
        // The GLB should apply the forEach on every element and keep an Exception for
        // the index "nullIndex"
        try {
            final ArrayList<Exception> ex = underGLB(() -> {
                final List<Throwable> errors = distCol.GLB.forEach(makeSuffixTest).getErrors();
                // We should have 1 exception in the errors returned by the GLB operation
                assertEquals(1, errors.size());

                // We check that the exception we got is indeed what we expect it to be
                final Throwable t = errors.get(0);
                // T is supposed to be the wrapper "DistColGlbError", which indicates that the
                // problem occurred on index "nullIndex"
                MatcherAssert.assertThat(t, IsInstanceOf.instanceOf(DistColGlb.DistColGlbError.class));
                assertEquals(nullIndex, ((DistColGlbError) t).index);
                // The cause should be a NullPointerException
                MatcherAssert.assertThat(t.getCause(), IsInstanceOf.instanceOf(NullPointerException.class));

                // We check that the operation was indeed applied to all other indices
                final List<Throwable> checkErrors = distCol.GLB.forEach((l, e) -> {
                    if (l == nullIndex) {
                        // Check that the element is indeed null
                        assertNull(e);
                    } else {

                        // Check that the element has the correct prefix
                        assertTrue(e.s.endsWith("Test"));
                    }
                }).getErrors();

                // Potential assertion failures would be caught in the previous forEach
                // operation's errors.
                assertTrue("There were <" + checkErrors.size() + "> errors when we expected <0>",
                        checkErrors.isEmpty());
            });
            assertEquals(0, ex.size()); // There shouldn't be any error thrown from inside the GLB
        } catch (final MultipleException me) {
            printExceptionAndThrowFirst(me);
        }
    }
}
