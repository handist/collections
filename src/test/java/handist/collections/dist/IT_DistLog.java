package handist.collections.dist;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Consumer;

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
    /**
     * Number of places this test is running on
     */
    static int NPLACES;

    final static long numSteps = 4;

    /**
     * Number of initial data entries places into the map
     */
    final static long numData = 5;

    /**
     * PlaceGroup on which the DistMap is defined on
     */
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

    static final String dir = "src/test/resources/DistLog/";
    static final String correctFile1 = dir+"correctFile1.txt";
    static final String correctFile2 = dir+"correctFile2.txt";
    static final String correctFile3 = dir+"correctFile3.txt";
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
            final DistLog dlog3 = DistLog.globalSetup(pg, 0, false);

            for (int step = 0; step < numSteps; step++) {
                final long phaseVal = step;
                pg.broadcastFlat(() -> {
                    finish(() -> {
                        for (int i = 0; i < numData; i++) {
                            final String tag = "tag" + i;
                            async(() -> {
                                DistLog.log(tag, "msg0", "appendix:" + System.currentTimeMillis());
                            });
                        }
                        for (int i = 0; i < numData; i++) {
                            String tag = "tag" + i;
                            String msg = "msg0";
                            dlog1.put(tag, msg, "time:" + System.currentTimeMillis());

                            if(phaseVal>1 && i==3) tag = tag + "X";
                            if(phaseVal>1 && i==2) msg = msg + "X";
                            dlog3.put(phaseVal, tag, msg, System.currentTimeMillis());
                        }
                    });
                });
                for (int p = 0; p < NPLACES; p++) {
                    for (int i = 0; i < numData; i++) {
                        final String tag = "tag" + i;
                        dlog2.put(tag, "msg0", "time:" + System.currentTimeMillis());
                    }
                }
                DistLog.defaultGlobalSetPhase(step+1);
                dlog1.globalSetPhase(step+1);
                dlog2.globalSetPhase(step+1);
            }
            DistLog.defaultGlobalGather();
            dlog1.globalGather();
            dlog3.globalGather();


            //DistLog.defaultLog.printAll(System.out);
            //System.out.println("This is a part of test");
            //dlog1.printAll(System.out);
            //System.out.println("This is a part of test, dlog2");
            //dlog2.printAll(System.out);
            //System.out.println("This is a part of test, dlog3");
            //dlog3.printAll(System.out);

            assertTrue(dlog1.placeConsciousEquals(DistLog.defaultLog, System.err, true));

            assertTrue(dlog1.placeConsciousEquals(DistLog.defaultLog, System.err, false));
            DistLog.LogItem.appendixPrint = false;
            compareOut((PrintStream out) -> assertFalse(dlog1.placeConsciousEquals(dlog2, out, false)),
                    correctFile1);
            assertTrue(dlog1.distributionFreeEquals(dlog2, System.out));

            compareOut((PrintStream out) -> assertFalse(dlog1.placeConsciousEquals(dlog3, out, false)),
                    correctFile2);

            compareOut((PrintStream out) -> assertFalse(dlog1.distributionFreeEquals(dlog3, out)),
                    correctFile3);

        } catch (final Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    private void compareOut(Consumer<PrintStream> func, String filename) {
        try {
            File file = new File(filename);
            if (file.exists()) {
                ByteArrayOutputStream out0 = new ByteArrayOutputStream();
                PrintStream out = new PrintStream(out0);
                func.accept(out);
                out.close();

                int count;
                BufferedReader in0 = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(out0.toByteArray())));
                BufferedReader in1 = new BufferedReader(new FileReader(file));
                String line0;
                count = 0;
                while ((line0 = in0.readLine()) != null) {
                    String line1 = in1.readLine();
                    String msg = "Diff fount in line " + count + ", result: "+ line0 + ", correct:" + line1;
                    assertEquals(msg, line0,line1);
                    count++;
                }
                String lineRem = in1.readLine();
                String msg2 = "Only correct has line " + count + ":" + lineRem;
                assertNull(msg2, lineRem);
            } else {
                PrintStream out = new PrintStream(new FileOutputStream(file));
                func.accept(out);
                out.close();
                System.err.println("Correct file " + file.getAbsolutePath() + " is not found. Now generating...");

            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() {
        NPLACES = pg.size();
    }

}
