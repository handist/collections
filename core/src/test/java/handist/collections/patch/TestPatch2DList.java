package handist.collections.patch;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import handist.collections.patch.Patch2DList.Edge;

public class TestPatch2DList {

    private static final int numData = 10;
    private static final int seed = 12345;

    private final Range2D range = new Range2D(new Position2D(0, 0), new Position2D(4, 2));
    private final int xSplit = 2;
    private final int ySplit = 2;
    private Position2D gridSize;

    private Patch2DList<Element> list;
    private Element[] elems;

    @Before
    public void setup() {
        list = new Patch2DList<>(new Patch2D<>(range), xSplit, ySplit);
        elems = new Element[numData];
        gridSize = new Position2D(range.size().x / xSplit, range.size().y / ySplit);

        final Random rand = new Random(seed);
        for (int i = 0; i < numData; i++) {
            elems[i] = new Element(rand.nextDouble() * range.rightTop.x, rand.nextDouble() * range.rightTop.y);
            list.put(elems[i]);
        }
    }

    @Test
    public void testContainsRange() {
        range.split(xSplit, ySplit).forEach((r) -> {
            assertTrue(list.containsRange(r));
        });
    }

    @Test
    public void testFindNeighbors() {
        final Patch2D<Element> center = list.getPatch(0, 0);
        final Collection<Patch2D<Element>> neighbors1 = list.findNeighbors(center, 1, Edge.loop);
        assertEquals(4, neighbors1.size());

        final Collection<Patch2D<Element>> neighbors2 = list.findNeighbors(center, 0, Edge.loop);
        assertEquals(center, neighbors2.iterator().next());

        final Collection<Patch2D<Element>> neighbors3 = list.findNeighbors(center, 2, Edge.loop);
        assertEquals(4, neighbors1.size());
    }

    @Test
    public void testForEach() {
        list.forEach((e) -> {
            assertTrue(list.contains(e));
            e.value++;
        });
        for (final Element e : elems) {
            assertEquals(1, e.value);
        }
    }

    @Test
    public void testGetIndex() {
        list.forEachPatch((patch) -> {
            final Index2D index = list.getIndex(patch);
            assertTrue(index.x >= 0 && index.x < xSplit);
            assertTrue(index.y >= 0 && index.y < ySplit);
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetIndexError() {
        list.getIndex(new Patch2D<>(range));
    }

    @Test
    public void testGetPatchWithIndex() {
        for (int x = 0; x < xSplit; x++) {
            for (int y = 0; y < ySplit; y++) {
                assertNotNull(list.getPatch(x, y));
                assertEquals(new Index2D(x, y), list.getIndex(list.getPatch(x, y)));
            }
        }
        assertNull(list.getPatch(-1, -1));
        assertNull(list.getPatch(100, 100));
    }

    @Test
    public void testGetPatchWithRange() {
        list.forEachPatch((patch) -> {
            assertEquals(patch.id(), list.getPatch(patch.getRange()).id());
        });
        assertNull(list.getPatch(new Range2D(new Position2D(1, 1), new Position2D(1 + gridSize.x, 1 + gridSize.y))));
        assertNull(list.getPatch(range));
    }

    @Test
    public void testGetPatchWithVector() {
        assertEquals(list.getPatch(0, 0), list.getPatch(new Position2D(0.00, 0.00)));
        assertEquals(list.getPatch(0, 0), list.getPatch(new Position2D(1.99, 0.99)));

        assertEquals(list.getPatch(1, 0), list.getPatch(new Position2D(2.00, 0.00)));
        assertEquals(list.getPatch(1, 0), list.getPatch(new Position2D(3.99, 0.99)));

        assertEquals(list.getPatch(0, 1), list.getPatch(new Position2D(0.00, 1.00)));
        assertEquals(list.getPatch(0, 1), list.getPatch(new Position2D(1.99, 1.99)));

        assertEquals(list.getPatch(1, 1), list.getPatch(new Position2D(2.00, 1.00)));
        assertEquals(list.getPatch(1, 1), list.getPatch(new Position2D(3.99, 1.99)));
    }

    @Test
    public void testGridSize() {
        assertEquals(gridSize, list.gridSize());
    }

    @Test
    public void testMigrate() throws Exception {
        list.forEach((element) -> {
            element.position = new Position2D(0, 0);
        });
        list.migrate();
        list.forEachPatch((patch) -> {
            final int expected = (patch.id() == 0) ? numData : 0;
            assertEquals(expected, patch.size());
        });
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testMigrateError() throws Exception {
        list.forEach((element) -> {
            element.position = new Position2D(0, -1);
        });
        list.migrate();
    }

    @Test
    public void testParallelForEachPatch() {
        list.parallelForEachPatch(2, (patch) -> {
            patch.forEach((e) -> {
                e.value++;
            });
        });
        for (final Element e : elems) {
            assertEquals(1, e.value);
        }

        list.parallelForEachPatch(1000, (patch) -> {
            patch.forEach((e) -> {
                e.value++;
            });
        });
        for (final Element e : elems) {
            assertEquals(2, e.value);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testParallelForEachPatchError() {
        list.parallelForEachPatch(0, (patch) -> {
        });
    }

}
