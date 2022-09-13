package handist.collections.patch;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.Iterator;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class TestPatch2D {

    private static final int numData = 4;
    private static final int seed = 12345;

    private final Range2D range = new Range2D(new Position2D(0, 0), new Position2D(2, 1));
    private Patch2D<Element> patch;

    private final Element[] elems = new Element[] { new Element(0, 0), new Element(0.5, 0.1), new Element(1, 0.2),
            new Element(1.5, 0.3) };

    @Before
    public void setup() {
        patch = new Patch2D<>(range);
        final Random rand = new Random(seed);
        for (int i = 0; i < numData; i++) {
            patch.put(elems[i]);
        }
    }

    @Test
    public void testClear() {
        assertEquals(numData, patch.size());
        patch.clear();
        assertEquals(0, patch.size());
        patch.forEach((e) -> {
            throw new AssertionError();
        });
    }

    @Test
    public void testContains() {
        for (int i = 0; i < numData; i++) {
            assertTrue(patch.contains(elems[i]));
        }
        assertFalse(patch.contains(null));
    }

    @Test
    public void testForEach() {
        final int[] count = { 0 };
        patch.forEach((e) -> {
            count[0]++;
        });
        assertEquals(numData, count[0]);
    }

    @Test
    public void testIterator() {
        int count = 0;
        final Iterator<Element> iter = patch.iterator();
        while (iter.hasNext()) {
            count++;
            iter.next();
        }
        assertEquals(numData, count);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPutError() {
        patch.put(new Element(2, 1));
    }

    @Test
    public void testRemove() {
        for (int i = 0; i < numData; i++) {
            assertTrue(patch.remove(elems[i]));
            assertEquals(numData - i - 1, patch.size());
        }
    }

    @Test
    public void testSize() {
        assertEquals(numData, patch.size());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testSplit() {
        final Collection<Patch2D<Element>> split = patch.split(2, 2);
        assertEquals(4, split.size());

        int count = 0;
        for (final Patch2D<Element> p : split) {
            for (final Element e : p) {
                assertTrue(p.getRange().contains(e.position));
                count++;
            }
        }
        assertEquals(numData, count);
    }
}
