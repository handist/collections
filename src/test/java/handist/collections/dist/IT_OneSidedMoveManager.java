package handist.collections.dist;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.function.SerializableFunction;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_OneSidedMoveManager implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -398958835306999538L;

    /** Number of ranges to populate this collection */
    protected final static long LONGRANGE_COUNT = 20l;

    /** Size of individual ranges */
    protected final static long RANGE_SIZE = 25l;

    /** Total number of elements contained in the {@link DistCol} */
    protected final static long TOTAL_DATA_SIZE = LONGRANGE_COUNT * RANGE_SIZE;

    /**
     * Flag used as part of a test to check if a certain action took place
     */
    protected static volatile boolean flag;

    /**
     * Helper method which fill the provided DistCol with values
     *
     * @param col the collection which needs to be populated
     */
    protected static void y_populateDistCol(DistCol<Element> col) {
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
     * Distributed collection used to test the facilities of
     * {@link OneSidedMoveManager}
     */
    protected DistCol<Element> col;

    /**
     * One sided move manager under test
     */
    protected OneSidedMoveManager manager;

    /**
     * Place to which objects are transferred
     */
    protected Place destination;

    /**
     *
     */
    @Before
    public void setup() {
        col = new DistCol<>();
        y_populateDistCol(col);
        destination = place(1);
        manager = new OneSidedMoveManager(destination);
    }

    /**
     * Cleanup after the test
     */
    @After
    public void tearDown() {
        col.destroy();
    }

    @Test(timeout = 20000)
    public void testAsynchronousOneSidedTransfer() throws Throwable {
        final AtomicLong givenAway = new AtomicLong(0l);
        col.forEachChunk(c -> {
            if ((c.getRange().from / 100) % 4 == 0) {
                col.moveRangeAtSync(c.getRange(), destination, manager);
                givenAway.addAndGet(c.size());
            }
        });

        finish(() -> {
            manager.asyncSend();
        });

        // Check that the distribution is correct
        final long given = givenAway.get();
        x_checkSize(p -> {
            switch (p.id) {
            case 0:
                return TOTAL_DATA_SIZE - given;
            case 1:
                return given;
            default:
                return 0l;
            }
        }, col);
    }

    @Test
    public void testRequestNonAuthorizedPlace() throws Throwable {
        assertThrows(RuntimeException.class, () -> col.forEachChunk(c -> {
            col.moveRangeAtSync(c.getRange(), place(3), manager);
        }));
    }

    @Test(timeout = 20000)
    public void testSynchronousSend() throws Throwable {
        final AtomicLong givenAway = new AtomicLong(0l);
        col.forEachChunk(c -> {
            if ((c.getRange().from / 100) % 4 == 0) {
                col.moveRangeAtSync(c.getRange(), destination, manager);
                givenAway.addAndGet(c.size());
            }
        });

        manager.send();

        // Check that the distribution is correct
        final long given = givenAway.get();
        x_checkSize(p -> {
            switch (p.id) {
            case 0:
                return TOTAL_DATA_SIZE - given;
            case 1:
                return given;
            default:
                return 0l;
            }
        }, col);
    }

    /**
     * Subroutine which checks that every place holds half of the total instances
     *
     * @param size    function indicating the size expected at each place on which
     *                it is defined
     * @param distCol the {@link DistCol} whose size is being checked.
     * @throws Throwable if thrown during the check
     */
    protected void x_checkSize(final SerializableFunction<Place, Long> size, DistCol<?> distCol) throws Throwable {
        try {
            distCol.placeGroup().broadcastFlat(() -> {
                final long expected = size.apply(here());
                try {
                    assertEquals(expected, distCol.size());
                } catch (final Throwable e) {
                    final RuntimeException re = new RuntimeException("Error on " + here());
                    re.initCause(e);
                    throw re;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }
}
