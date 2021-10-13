package handist.collections.glb.util;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;

import apgas.impl.Config;
import apgas.impl.DebugFinish;
import handist.collections.dist.DistLog;
import handist.collections.dist.DistLog.LogItem;
import handist.collections.dist.DistLog.LogKey;
import handist.collections.util.SavedLog;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_GlbLog implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -2415317881311337618L;

    static final String TOPIC_A = "topicA";
    static final String TOPIC_B = "topicB";
    static final String TOPIC_C = "topicC";

    static final String GLBLOG_FILENAME = IT_GlbLog.class.getName() + "_tmpFile.txt";

    /**
     * Original DistLog instance into which information is recorded.
     */
    public DistLog distLog;

    /**
     * Instance into which all the distributed logs of DistLog are gathered back
     * into. This object is the main subject of this test class
     */
    public SavedLog glbLog;

    @Rule
    public transient TestName nameOfCurrentTest = new TestName();

    @After
    public void afterEachTest() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (DebugFinish.class.getCanonicalName().equals(System.getProperty(Config.APGAS_FINISH))) {
            System.err.println("Dumping the errors that occurred during " + nameOfCurrentTest.getMethodName());
            // If we are using the DebugFinish, dump all throwables collected on each host
            DebugFinish.dumpAllSuppressedExceptions();
        }
    }

    /**
     * In this method, a clean instance of {@link #distLog} is initialized and any
     * previous {@link #glbLog} instance is discarded
     */
    @Before
    public void beforeEachTest() {
        final DistLog dLog = new DistLog();
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            glbLog = null;
            distLog = dLog;

            // Initialize distLog with some values depending on the place:
            switch (here().id) {
            case 0:
            case 1:
                // place(0) and place(1)
                distLog.put(TOPIC_A, "first message", Long.toString(System.nanoTime()));
                distLog.put(TOPIC_A, "second message", Long.toString(System.nanoTime()));
                distLog.put(TOPIC_B, "third message", null);
                break;
            case 2:
                distLog.put(TOPIC_C, "a", null);
                distLog.put(TOPIC_C, "b", null);
                distLog.put(TOPIC_C, "c", null);
                break;
            case 3:
            default:
                // no logs made on place(3)
            }
        });

        // Gather all logged entries on Place 0
        distLog.globalGather();
        // Create a GlbLog based on this DistLog
        glbLog = new SavedLog(distLog);
    }

    /**
     * Checks if storing a {@link SavedLog} instance to a file and restoring an
     * instance from this recording preserves the properties of the instance
     *
     * @throws IOException            if thrown during the execution of the test
     * @throws ClassNotFoundException if thrown during the execution of the test
     */
    @Test
    public void checkFileRecordConsistency() throws IOException, ClassNotFoundException {
        // Save glbLog to a file on the system
        final File f = new File(GLBLOG_FILENAME);
        f.deleteOnExit();
        glbLog.saveToFile(f);

        final File fileForInput = new File(GLBLOG_FILENAME);
        assertTrue(fileForInput.exists());
        assertTrue(fileForInput.canRead());

        final SavedLog logFromFile = new SavedLog(fileForInput);

        // Check that every key in DistLog has its equivalent in the GlbLog
        final Collection<LogKey> keys = distLog.getDistMultiMap().keySet();
        for (final LogKey key : keys) {
            assertNotNull(glbLog.getLog(key));
            assertNotNull(logFromFile.getLog(key));
        }

        assertTrue(logFromFile.equals(glbLog));
        assertTrue(logFromFile.equals(distLog));
    }

    /**
     * Checks that a {@link SavedLog} instance created from a {@link DistLog} contains
     * all its logged instances
     */
    @Test
    public void checkGlbLogMatchesDistLog() {
        assertTrue(glbLog.equals(distLog));
        final Collection<LogKey> keys = distLog.getDistMultiMap().keySet();
        for (final LogKey k : keys) {
            final Collection<LogItem> distributedLogs = distLog.getLog(k);
            final Collection<LogItem> glbLogs = glbLog.getLog(k);

            assertNotNull(distributedLogs);
            assertNotNull(glbLogs);
            assertSame(distributedLogs, glbLogs);
        }

        distLog.put(TOPIC_C, "a additional message", null);

        assertFalse(glbLog.equals(distLog));
    }

}
