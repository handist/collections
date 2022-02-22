package handist.collections;

import static apgas.Constructs.*;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.Place;
import apgas.SerializableJob;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_RangedListProduct {

    final static TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();
    final static int numThreads = 1;
    final static long seed = 42;

    @Test
    public void testSplitProduct() throws Exception {
        final SerializableJob printProduct = () -> {
            final LongRange firstRange = new LongRange(0, 100);
            final LongRange secondRange = new LongRange(0, 200);
            final Chunk<Long> first = new Chunk<>(firstRange, l -> l);
            final Chunk<Long> second = new Chunk<>(secondRange, l -> l);
            final RangedListProduct<Long, Long> product = RangedListProduct.newProduct(first, second);
            final List<List<RangedListProduct<Long, Long>>> split = product.teamedSplitNM(2, 2, world, numThreads,
                    seed);
            for (int i = 0; i < split.size(); i++) {
                final List<RangedListProduct<Long, Long>> list = split.get(i);
                System.err.print("List " + i);
                for (int j = 0; j < list.size(); j++) {
                    System.err.print(" " + list.get(j).getRange());
                }
                System.err.println();
            }
        };

        for (final Place p : places()) {
            System.err.println("Product on " + p);
            at(p, () -> printProduct.run());
        }
    }
}
