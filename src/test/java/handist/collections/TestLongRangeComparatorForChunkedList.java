package handist.collections;

import static org.junit.Assert.*;

import java.util.Comparator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.junit.Before;
import org.junit.Test;

/**
 * Checks the ordering used in class {@link ChunkedList} to store chunks in the
 * underlying {@link ConcurrentSkipListMap}.
 *
 * @author Patrick Finnerty
 *
 */
public class TestLongRangeComparatorForChunkedList {

    /** Instance under test */
    Comparator<LongRange> comparator;

    LongRange range0to10;
    LongRange range0to5;
    LongRange range5;
    LongRange range5to10;

    @Before
    public void setup() {
        comparator = new ChunkedList.LongRangeOrdering();
        range0to10 = new LongRange(0l, 10l);
        range0to5 = new LongRange(0l, 5l);
        range5to10 = new LongRange(5l, 10l);
        range5 = new LongRange(5l);
    }

    @Test
    public void testOrdering() {
        // Equality cases
        assertEquals(0, comparator.compare(range0to10, range0to10));
        assertEquals(0, comparator.compare(range0to5, range0to5));
        assertEquals(0, comparator.compare(range5, range5));

        // First argument smaller than the second
        assertTrue((comparator.compare(range0to10, range0to5)) < 0);
        assertTrue((comparator.compare(range0to10, range5)) < 0);
        assertTrue((comparator.compare(range5to10, range5)) < 0);
    }
}
