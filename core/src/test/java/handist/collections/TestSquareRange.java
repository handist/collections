package handist.collections;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSquareRange {

    private final LongRange outerRange = new LongRange(1, 10);
    private final LongRange innerRange = new LongRange(2, 10);

    private SquareRange square;
    /**
     * &ensp;&nbsp;123456789<br>
     * 1 011111111<br>
     * 2 001111111<br>
     * 3 000111111<br>
     * 4 000011111<br>
     * 5 000001111<br>
     * 6 000000111<br>
     * 7 000000011<br>
     * 8 000000001<br>
     * 9 000000000<br>
     */
    private SquareRange upperTriangle;

    @Before
    public void setup() {
        square = new SquareRange(outerRange, innerRange);
        upperTriangle = new SquareRange(outerRange, innerRange, true);
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
        assertTrue(square.contains(new SquareRange(new LongRange(2, 9), new LongRange(3, 9))));
        assertFalse(square.contains(new SquareRange(new LongRange(-1, 9), new LongRange(3, 9))));
        assertFalse(square.contains(new SquareRange(new LongRange(2, 9), new LongRange(-1, 9))));
        assertFalse(square.contains(new SquareRange(new LongRange(-5, -1), new LongRange(-5, -1))));
        assertFalse(square.contains(new SquareRange(new LongRange(10, 15), new LongRange(10, 15))));
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
        // check upperTriangle
        assertEquals(new SquareRange(new LongRange(3, 7), new LongRange(3, 7), true),
                upperTriangle.intersection(new SquareRange(new LongRange(3, 7), new LongRange(3, 7), true, 2)));
    }

    @Test
    public void testIsOverlapped() {
        // square with square
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(1, 10), new LongRange(2, 10))));
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(2, 9), new LongRange(3, 9))));
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(-10, 2), new LongRange(3, 9))));
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(2, 9), new LongRange(9, 20))));
        assertFalse(square.isOverlapped(new SquareRange(new LongRange(-10, 1), new LongRange(3, 9))));
        assertFalse(square.isOverlapped(new SquareRange(new LongRange(2, 9), new LongRange(10, 20))));
        // square with triangle
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(1, 3), new LongRange(9, 11), true)));
        assertTrue(square.isOverlapped(new SquareRange(new LongRange(0, 2), new LongRange(8, 10), true)));
        // triangle with square
        assertTrue(upperTriangle.isOverlapped(new SquareRange(new LongRange(1, 10), new LongRange(2, 10))));
        assertTrue(upperTriangle.isOverlapped(new SquareRange(new LongRange(3, 5), new LongRange(3, 5))));
        assertFalse(upperTriangle.isOverlapped(new SquareRange(new LongRange(4, 6), new LongRange(3, 5))));
        assertFalse(upperTriangle.isOverlapped(new SquareRange(new LongRange(3, 5), new LongRange(2, 4))));
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
    }

    @Test
    public void testSize() {
        assertEquals(outerRange.size() * innerRange.size(), square.size());
        assertEquals(36, upperTriangle.size());
        assertEquals(199, new SquareRange(new LongRange(0, 2), new LongRange(0, 100), true).size());
        assertEquals(3, new SquareRange(new LongRange(0, 100), new LongRange(0, 2), true).size());
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
