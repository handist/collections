package handist.collections.glb;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.ExtendedConstructs;
import handist.collections.dist.DistCol;
import handist.collections.dist.IT_OneSidedMoveManager;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test class for the customized class in charge of performing the transfer of
 * objects between hosts. This test inherits the ones from
 * {@link IT_OneSidedMoveManager} to ensure that this customized version has not
 * lost any functionality compared to its parent implementation.
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_CustomMoveManager extends IT_OneSidedMoveManager implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -5294814955826374667L;

    @Before
    public void setUp() throws Exception {
        col = new DistCol<>();
        y_populateDistCol(col);
        destination = place(1);
        manager = new CustomOneSidedMoveManager(destination);
    }

    @Test(timeout = 20000)
    public void testAsyncSendAndDoNoMPI() throws Throwable {
        final CustomOneSidedMoveManager m = (CustomOneSidedMoveManager) manager;

        final AtomicLong givenAway = new AtomicLong(0l);
        col.forEachChunk(c -> {
            if ((c.getRange().from / 100) % 4 == 0) {
                col.moveRangeAtSync(c.getRange(), destination, m);
                givenAway.addAndGet(c.size());
            }
        });

        flag = false;

        finish(() -> {
            m.asyncSendAndDoNoMPI(() -> {
                asyncAt(place(0), () -> {
                    IT_OneSidedMoveManager.flag = true;
                });
            }, ExtendedConstructs.currentFinish());
        });

        assertTrue(flag);

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

    @Test(timeout = 20000)
    public void testAsyncSendAndDoWithMPI() throws Throwable {
        final CustomOneSidedMoveManager m = (CustomOneSidedMoveManager) manager;

        final AtomicLong givenAway = new AtomicLong(0l);
        col.forEachChunk(c -> {
            if ((c.getRange().from / 100) % 4 == 0) {
                col.moveRangeAtSync(c.getRange(), destination, m);
                givenAway.addAndGet(c.size());
            }
        });

        flag = false;

        finish(() -> {
            m.asyncSendAndDoWithMPI(() -> {
                asyncAt(place(0), () -> {
                    IT_OneSidedMoveManager.flag = true;
                });
            }, ExtendedConstructs.currentFinish());
        });

        assertTrue(flag);

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
}
