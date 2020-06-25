package handist.collections.dist;

import static apgas.Constructs.*;

public class TestTeamedPlaceGroup {
    static {
        TeamedPlaceGroup.setup();
    }

    public static void main(String[] args) {
        TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();

        finish(() -> {
            world.broadcastFlat(() -> {
                System.out.println("hello:" + here() + ", " + world);
                TeamedPlaceGroup split = world.splitHalf();
                System.out.println("split hello:" + here() + ", " + split);
                if (split.myrank() == 0) {
                    if (world.myrank() == 0) {
                        new TestDistMap(split).run();
                        System.out.println("----finishA");
                    } else {
                        new TestDistMapList(split).run();
                        System.out.println("----finishB");
                    }
                }
            });
        });
        System.out.println("----finish");
    }
}
