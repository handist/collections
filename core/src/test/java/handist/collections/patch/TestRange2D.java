package handist.collections.patch;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Test;

public class TestRange2D {

    private final Range2D range = new Range2D(new Position2D(0, 0), new Position2D(2, 1));

    @Test
    public void testContains() {
        for (int i = 0; i < 2; i++) {
            assertTrue(range.contains(new Position2D(i, i * 0.5)));
        }

        assertFalse(range.contains(new Position2D(2, 1)));
        assertFalse(range.contains(new Position2D(2, 0.5)));
        assertFalse(range.contains(new Position2D(1, 1)));
    }

    @Test
    public void testEquals() {
        assertEquals(new Range2D(new Position2D(0, 0), new Position2D(2, 1)), range);
    }

    @Test
    public void testSize() {
        assertEquals(new Position2D(2, 1), range.size());
    }

    @Test
    public void testSplit() {
        final Collection<Range2D> ranges = range.split(2, 2);
        // split ranges are ordered following. (x: inner loop / y: outer loop)
        int i = 0;
        for (final Range2D r : ranges) {
            final double x1 = (i % 2 == 0) ? 0 : 1;
            final double y1 = (i < 2) ? 0 : 0.5;
            assertEquals(new Range2D(new Position2D(x1, y1), new Position2D(x1 + 1, y1 + 0.5)), r);
            i++;
        }
    }
}
