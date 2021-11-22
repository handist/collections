package handist.collections;

import static org.junit.Assert.*;

import java.io.Serializable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import handist.collections.dist.util.Pair;

public class TestRangedListProduct implements Serializable {

    /**
     * The first class used as element in the tests. Have a long value.
     */
    private class First implements Serializable {
        private static final long serialVersionUID = -8082003165166213276L;
        public long value;

        public First(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "First (" + value + ")";
        }
    }

    /**
     * The second class used as element in the tests. Have long values.
     */
    private class Second implements Serializable {
        private static final long serialVersionUID = -3693548438531803014L;
        public long value = 0;

        public Second(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return "Second (" + value + ")";
        }
    }

    private static final long serialVersionUID = -7848690131967719062L;

    /** product of listFirst[1,10), listSecond[2,10) */
    private RangedListProduct<First, Second> prod;
    /** triangle product of listFirst[1,10), listSecond[2,10) */
    @SuppressWarnings("unused")
    private RangedListProduct<First, Second> triangle;

    /** [1,10) range list, value is "10 + index" */
    private RangedList<First> listFirst;
    /** [2,10) range list, value is "20 + index" */
    private RangedList<Second> listSecond;

    private final LongRange rangeFirst = new LongRange(1, 10);
    private final LongRange rangeSecond = new LongRange(2, 10);

    @Before
    public void setup() {
        listFirst = new Chunk<>(rangeFirst, (index) -> {
            return new First(10 + index);
        });
        listSecond = new Chunk<>(rangeSecond, (index) -> {
            return new Second(20 + index);
        });
        prod = new RangedListProduct<>(listFirst, listSecond);
        triangle = new RangedListProduct<>(listFirst, listSecond, true);
    }

    @After
    public void tearDown() {
    }

    @Test
    public void testAsColumnList() {
        final RangedList<RangedList<Pair<First, Second>>> columnList = prod.asColumnList();
        assertEquals(rangeSecond, columnList.getRange());

        columnList.forEach((long cIndex, RangedList<Pair<First, Second>> column) -> {
            assertEquals(rangeFirst, column.getRange());
            column.forEach((long rIndex, Pair<First, Second> pair) -> {
                assertEquals(10 + rIndex, pair.first.value);
                assertEquals(20 + cIndex, pair.second.value);
            });
        });
    }

    @Test
    public void testAsRowList() {
        final RangedList<RangedList<Pair<First, Second>>> rowList = prod.asRowList();
        assertEquals(rangeFirst, rowList.getRange());

        rowList.forEach((long rIndex, RangedList<Pair<First, Second>> row) -> {
            assertEquals(rangeSecond, row.getRange());
            row.forEach((long cIndex, Pair<First, Second> pair) -> {
                assertEquals(10 + rIndex, pair.first.value);
                assertEquals(20 + cIndex, pair.second.value);
            });
        });
    }

    @Test
    public void testGetRange() {
        final SquareRange expected = new SquareRange(rangeFirst, rangeSecond);
        assertEquals(expected, prod.getRange());
    }

}
