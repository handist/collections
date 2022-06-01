package handist.collections;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSquareRange {

    private final LongRange outerRange = new LongRange(1, 10);
    private final LongRange innerRange = new LongRange(2, 10);

    private final SquareRange square = new SquareRange(outerRange, innerRange);
    /**
     * &ensp;&nbsp;23456789<br>
     * 1 11111111<br>
     * 2 01111111<br>
     * 3 00111111<br>
     * 4 00011111<br>
     * 5 00001111<br>
     * 6 00000111<br>
     * 7 00000011<br>
     * 8 00000001<br>
     * 9 00000000<br>
     */
    private final SquareRange upperTriangle = new SquareRange(outerRange, innerRange, true);

    /**
     * &ensp;&nbsp;01<br>
     * 8 00<br>
     * 9 00<br>
     */
    private final SquareRange upperTriangle_empty = new SquareRange(new LongRange(8, 10), new LongRange(0, 2), true);

    private final SquareRange topLeft = new SquareRange(new LongRange(0, 2), new LongRange(1, 3));
    private final SquareRange topRight = new SquareRange(new LongRange(0, 2), new LongRange(9, 11));
    private final SquareRange bottomLeft = new SquareRange(new LongRange(9, 11), new LongRange(1, 3));
    private final SquareRange bottomRight = new SquareRange(new LongRange(9, 11), new LongRange(9, 11));
    private final SquareRange center = new SquareRange(new LongRange(4, 6), new LongRange(4, 6));
    private final SquareRange large = new SquareRange(new LongRange(0, 11), new LongRange(1, 11));

    @Before
    public void setup() {

    }

    @After
    public void tearDown() {
    }

    @Test
    public void testColumnRange() {
        // check all row
        int i = 0;
        for (long row = outerRange.from; row < outerRange.to; row++) {
            assertEquals(innerRange, square.columnRange(row));
            assertEquals(new LongRange(innerRange.from + i, innerRange.to), upperTriangle.columnRange(row));
            i++;
        }
        // check uppertriangle empty
        for (long row = 8; row < 10; row++) {
            assertEquals(new LongRange(2, 2), upperTriangle_empty.columnRange(row));
        }
    }

    @Test
    public void testContains() {
        assertTrue(square.contains(9, 9));
        assertTrue(square.contains(1, 2));
        assertFalse(square.contains(10, 10));
        assertFalse(square.contains(0, 1));
        assertFalse(square.contains(-1, 5));
        assertFalse(square.contains(5, -1));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testContainsCheck() {
        square.containsCheck(-1, 1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testContainsCheckWithRange() {
        square.containsCheck(new SquareRange(new LongRange(-1, 1), new LongRange(-1, 1)));
    }

    @Test
    public void testContainsWithRange() {
        assertTrue(square.contains(new SquareRange(outerRange, innerRange)));
        assertTrue(square.contains(center));
        assertFalse(square.contains(topLeft));
        assertFalse(square.contains(topRight));
        assertFalse(square.contains(bottomLeft));
        assertFalse(square.contains(bottomRight));
        assertFalse(square.contains(large));

        assertTrue(square.contains(new SquareRange(outerRange, innerRange, true)));
        assertTrue(square.contains(new SquareRange(center.outer, center.inner, true)));
        assertFalse(upperTriangle.contains(center));
        assertFalse(upperTriangle.contains(topLeft));
        assertFalse(upperTriangle.contains(topRight));
        assertFalse(upperTriangle.contains(bottomLeft));
        assertFalse(upperTriangle.contains(bottomRight));
        assertFalse(upperTriangle.contains(large));
    }

    @Test
    public void testEndRow() {
        for (int column = (int) innerRange.from; column < innerRange.to; column++) {
            assertEquals(outerRange.to, square.endRow(column));
            assertEquals(column, upperTriangle.endRow(column));
        }
    }

    @Test
    public void testIntersection() {
        assertEquals(new SquareRange(new LongRange(1, 10), new LongRange(2, 10), true),
                square.intersection(upperTriangle));
        assertEquals(new SquareRange(new LongRange(1, 10), new LongRange(2, 10), true),
                upperTriangle.intersection(square));
        // check upperTriangles with triangle diff
        assertEquals(new SquareRange(new LongRange(4, 6), new LongRange(4, 6), true),
                upperTriangle.intersection(new SquareRange(new LongRange(4, 6), new LongRange(4, 6), true, 2)));
        // check intersections of the upper triangle and square (in the lower triangle).
        assertNull(upperTriangle.intersection(bottomLeft));
        // check intersections of upper triangle and no intersections at all.
        assertNull(upperTriangle.intersection(new SquareRange(new LongRange(10, 11), new LongRange(0, 2))));
    }

    @Test
    public void testIsOverlapped() {
        // square with square
        assertTrue(square.isOverlapped(square));
        assertTrue(square.isOverlapped(center));
        assertTrue(square.isOverlapped(topLeft));
        assertTrue(square.isOverlapped(topRight));
        assertTrue(square.isOverlapped(bottomLeft));
        assertTrue(square.isOverlapped(bottomRight));
        // square with triangle
        assertTrue(upperTriangle.isOverlapped(center));
        assertTrue(upperTriangle.isOverlapped(topLeft));
        assertTrue(upperTriangle.isOverlapped(topRight));
        assertFalse(upperTriangle.isOverlapped(bottomLeft));
        assertTrue(upperTriangle.isOverlapped(bottomRight));
        // square with triangle (with triangle diff)
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(1, 5), new LongRange(2, 5), true, 1)));
        assertFalse(square.isOverlapped(new SquareRange(new LongRange(1, 5), new LongRange(2, 5), true, -10)));
    }

    @Test
    public void testRowRange() {
        // check all column
        int i = 1;
        for (long column = innerRange.from; column < innerRange.to; column++) {
            assertEquals(outerRange, square.rowRange(column));
            assertEquals(new LongRange(outerRange.from, outerRange.from + i), upperTriangle.rowRange(column));
            i++;
        }
        // check uppertriangle empty
        for (long column = 0; column < 2; column++) {
            assertEquals(new LongRange(8, 8), upperTriangle_empty.rowRange(column));
        }
    }

    @Test
    public void testSize() {
        assertEquals(outerRange.size() * innerRange.size(), square.size());
        assertEquals(36, upperTriangle.size());
        assertEquals(0, upperTriangle_empty.size());
        assertEquals(197, new SquareRange(new LongRange(0, 2), new LongRange(0, 100), true).size());
        assertEquals(1, new SquareRange(new LongRange(0, 100), new LongRange(0, 2), true).size());
        assertEquals(199, new SquareRange(new LongRange(0, 2), new LongRange(0, 100), true, 1).size());
        assertEquals(3, new SquareRange(new LongRange(0, 100), new LongRange(0, 2), true, 1).size());
    }

    @Test
    public void testSplit() {
        final List<SquareRange> split = square.split(3, 2);
        assertEquals(new SquareRange(new LongRange(1, 4), new LongRange(2, 6)), split.get(0));
        assertEquals(new SquareRange(new LongRange(1, 4), new LongRange(6, 10)), split.get(1));
        assertEquals(new SquareRange(new LongRange(4, 7), new LongRange(2, 6)), split.get(2));
        assertEquals(new SquareRange(new LongRange(4, 7), new LongRange(6, 10)), split.get(3));
        assertEquals(new SquareRange(new LongRange(7, 10), new LongRange(2, 6)), split.get(4));
        assertEquals(new SquareRange(new LongRange(7, 10), new LongRange(6, 10)), split.get(5));

    }
}
