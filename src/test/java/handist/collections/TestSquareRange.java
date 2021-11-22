package handist.collections;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestSquareRange {

    private SquareRange square;
    private SquareRange upperTriangle;
    private final LongRange outerRange = new LongRange(1, 10);
    private final LongRange innerRange = new LongRange(2, 10);

    @Before
    public void setup() {
        square = new SquareRange(outerRange, innerRange);
        upperTriangle = new SquareRange(outerRange, innerRange, true);
    }

    @After
    public void tearDown() {
    }

    @Ignore
    @Test
    public void testColumnRange() {
        for (long row = outerRange.from; row < outerRange.to; row++) {
            assertEquals(innerRange, square.columnRange(row));
            assertEquals(innerRange.size() - row, upperTriangle.columnRange(row).size());
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
    public void testRowRange() {
        for (long column = innerRange.from; column < innerRange.to; column++) {
            assertEquals(outerRange, square.rowRange(column));
            assertEquals(column - innerRange.from, upperTriangle.rowRange(column).size());
        }
    }
}
