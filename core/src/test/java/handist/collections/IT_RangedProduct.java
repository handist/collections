package handist.collections;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.SerializableJob;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_RangedProduct {

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
            final RangedProduct<Long, Long> product = RangedProduct.newProd(first, second);
            final RangedProductList<Long, Long> split = product.teamedSplit(2, 2, world, seed);
            split.forEachProd((prod) -> {
                assertEquals(firstRange.size() / 2, prod.getRange().outer.size());
                assertEquals(secondRange.size() / 2, prod.getRange().inner.size());
            });
        };
    }
}
