package handist.collections.dist;

import java.io.Serializable;

import org.junit.Ignore;
import org.junit.runner.RunWith;

import handist.collections.dist.DistBag.DistBagGlobal;
import handist.collections.dist.DistBag.DistBagTeam;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test class dedicated to class {@link DistBag}, {@link DistBagGlobal}, and
 * {@link DistBagTeam}
 */
@Ignore
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistBag2 implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 8208565684336617060L;

    // TODO implement tests for the various features of class DistBag
}
