package handist.collections;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

public class TestLongRangeSet implements Serializable {

    private static final long serialVersionUID = -5548005710553675459L;

    /** SortedRangeSet filled with 3 initial members */
    private LongRangeSet rangeSet;
    /** initial members for rangeSet */
    private final LongRange[] ranges = { new LongRange(0, 10), new LongRange(15, 20), new LongRange(20, 30) };

    @Before
    public void setup() {
        rangeSet = new LongRangeSet();
        for (final LongRange r : ranges) {
            rangeSet.add(r);
        }
    }

    @Test
    public void testAddAll() {
        final TreeSet<LongRange> set = new TreeSet<>();
        set.add(new LongRange(-30, -20));
        set.add(new LongRange(-20, -10));
        assertTrue(rangeSet.addAll(set));
        assertEquals(5, rangeSet.size());

        set.add(new LongRange(-10, 0));
        assertTrue(rangeSet.addAll(set));
        assertEquals(6, rangeSet.size());

        assertFalse(rangeSet.addAll(set));
    }

    @Test
    public void testAddFalse() {
        for (final LongRange r : ranges) {
            assertFalse(rangeSet.add(new LongRange(r.from, r.from + 1)));
            assertFalse(rangeSet.add(new LongRange(r.to - 1, r.to)));
        }
        assertFalse(rangeSet.add(new LongRange(ranges[0].to, 1000)));
        assertFalse(rangeSet.add(new LongRange(-1000, ranges[1].from)));
        assertFalse(rangeSet.add(new LongRange(-1000, 1000)));
        assertEquals(3, rangeSet.size());
    }

    @Test
    public void testAddTrue() {
        assertTrue(rangeSet.add(new LongRange(ranges[0].from - 1, ranges[0].from)));
        assertTrue(rangeSet.add(new LongRange(ranges[0].to, ranges[1].from)));
        assertTrue(rangeSet.add(new LongRange(ranges[2].to)));
        assertEquals(6, rangeSet.size());
    }

    @Test
    public void testClear() {
        rangeSet.clear();
        assertTrue(rangeSet.isEmpty());
    }

    @Test
    public void testContainsIndex() {
        for (final LongRange r : ranges) {
            for (final Long i : r) {
                assertTrue(rangeSet.containsIndex(i));
            }
        }
        assertFalse(rangeSet.containsIndex(14));
        assertTrue(rangeSet.containsIndex(15));

        assertTrue(rangeSet.containsIndex(19));
        assertTrue(rangeSet.containsIndex(20));

        assertFalse(rangeSet.containsIndex(-1));
        assertFalse(rangeSet.containsIndex(30));
    }

    @Test
    public void testGetOverlap() {
        assertNull(rangeSet.getOverlap(-1));
        assertEquals(ranges[0], rangeSet.getOverlap(0));
        assertEquals(ranges[0], rangeSet.getOverlap(9));
        assertNull(rangeSet.getOverlap(10));

        assertNull(rangeSet.getOverlap(14));
        assertEquals(ranges[1], rangeSet.getOverlap(15));
        assertEquals(ranges[1], rangeSet.getOverlap(19));
        assertEquals(ranges[2], rangeSet.getOverlap(20));
        assertEquals(ranges[2], rangeSet.getOverlap(29));
        assertNull(rangeSet.getOverlap(30));
    }

    @Test
    public void testHeadSet() {
        SortedSet<LongRange> headSet = rangeSet.headSet(21, true);
        assertEquals(ranges[2], headSet.last());
        assertEquals(3, headSet.size());

        headSet = rangeSet.headSet(21, false);
        assertEquals(ranges[1], headSet.last());
        assertEquals(2, headSet.size());

        headSet = rangeSet.headSet(20, true);
        assertEquals(ranges[1], headSet.last());
        assertEquals(2, headSet.size());

        headSet = rangeSet.headSet(20, false);
        assertEquals(ranges[1], headSet.last());
        assertEquals(2, headSet.size());

        headSet = rangeSet.headSet(19, true);
        assertEquals(ranges[1], headSet.last());
        assertEquals(2, headSet.size());

        headSet = rangeSet.headSet(19, false);
        assertEquals(ranges[0], headSet.last());
        assertEquals(1, headSet.size());
    }

    @Test
    public void testSplit() {
        Collection<LongRange> split = rangeSet.split(5, 20);
        assertEquals(2, split.size());
        assertEquals(4, rangeSet.size());

        setup();
        split = rangeSet.split(5, 19);
        assertEquals(2, split.size());
        assertEquals(5, rangeSet.size());

        setup();
        split = rangeSet.split(5, 21);
        assertEquals(3, split.size());
        assertEquals(5, rangeSet.size());

        setup();
        split = rangeSet.split(20, 25);
        assertEquals(1, split.size());
        assertEquals(4, rangeSet.size());

        setup();
        split = rangeSet.split(19, 25);
        assertEquals(2, split.size());
        assertEquals(5, rangeSet.size());

        setup();
        split = rangeSet.split(21, 25);
        assertEquals(1, split.size());
        assertEquals(5, rangeSet.size());

        split = rangeSet.split(-100, -90);
        assertTrue(split.isEmpty());
    }

    @Test
    public void testTailSet() {
        SortedSet<LongRange> tailSet = rangeSet.tailSet(21, true);
        assertEquals(ranges[2], tailSet.first());
        assertEquals(1, tailSet.size());

        tailSet = rangeSet.tailSet(21, false);
        assertTrue(tailSet.isEmpty());

        tailSet = rangeSet.tailSet(20, true);
        assertEquals(ranges[2], tailSet.first());
        assertEquals(1, tailSet.size());

        tailSet = rangeSet.tailSet(20, false);
        assertTrue(tailSet.isEmpty());

        tailSet = rangeSet.tailSet(19, true);
        assertEquals(ranges[1], tailSet.first());
        assertEquals(2, tailSet.size());

        tailSet = rangeSet.tailSet(19, false);
        assertEquals(ranges[2], tailSet.first());
        assertEquals(1, tailSet.size());
    }

}
