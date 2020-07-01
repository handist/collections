package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.Serializable;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks=4, launcher=TestLauncher.class)
public class IT_TeamedPlaceGroup implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = -1060428318155098035L;

	TeamedPlaceGroup world;

	@Before
	public void setup() {
		world = TeamedPlaceGroup.getWorld();
	}

	@Test
	public void testSplitWorldInHalf() {
		finish(() -> {
			world.broadcastFlat(() -> {
				System.out.println("hello:" + here() + ", " + world);
				TeamedPlaceGroup split = world.splitHalf();
				System.out.println("split hello:" + here() + ", " + split);
				if (split.myrank() == 0) {
					if (world.myrank() == 0) {
						IT_DistMap test = new IT_DistMap();
						test.setup();
						test.placeGroup = split; //TODO this is not very clean
						test.run();
						System.out.println("----finishA");
					} else {
						IT_DistMapList test = new IT_DistMapList();
						test.setup();
						test.placeGroup = split; //TODO this is not very clean
						test.distMapList = new DistMapList<String, String>(split);
						test.run();
						System.out.println("----finishB");
					}
				}
			});
		});
		System.out.println("----finish");
	}
}
