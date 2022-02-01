package handist.collections;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

public class TestRangedMap implements Serializable {

    public static class Element implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = 5318079351127834274L;
        public int n = 0;

        public Element(int i) {
            n = i;
        }

        public Element increase(int i) {
            n += i;
            return this;
        }

        @Override
        public String toString() {
            return String.valueOf(n);
        }
    }

    private static final long serialVersionUID = -318126152971015421L;

    private static final int mapSize = 6;

    /**  */
    private RangedMap<Element> rangedMap;
    /** */
    private LongRange[] ranges;
    /** Contains 6 initialized instances of class Element */
    Element[] elems = new Element[mapSize];

    @Before
    public void setup() {
        rangedMap = new RangedMap<>();
        ranges = new LongRange[] { new LongRange(0, 3), new LongRange(3, 5), new LongRange(5, 6) };
        for (int i = 0; i < 3; i++) {
            rangedMap.addRange(ranges[i]);
            for (final long j : ranges[i]) {
                elems[(int) j] = new Element((int) j);
            }
        }
    }

    @Test
    public void testClear() {
        assertEquals(mapSize, rangedMap.size());
        rangedMap.clear();
        assertEquals(0l, rangedMap.size());
        assertEquals(0, rangedMap.count());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testClone() {
        final RangedMap<Element> clone1 = (RangedMap<Element>) rangedMap.clone();
        assertEquals(mapSize, clone1.size());
        assertEquals(0, clone1.count());
        testPut();

        final RangedMap<Element> clone2 = (RangedMap<Element>) rangedMap.clone();
        assertEquals(mapSize, clone2.size());
        assertEquals(elems.length, clone2.count());
        for (int i = 0; i < elems.length; i++) {
            assertEquals(elems[i], clone2.get((long) i));
        }

        rangedMap.addRange(new LongRange(10, 15));
        assertEquals(mapSize, clone2.size());
    }

    @Test
    public void testCount() {
        assertEquals(0l, rangedMap.count());
        testPut();
        assertEquals(elems.length, rangedMap.count());
        rangedMap.addRange(new LongRange(10, 15));
        assertEquals(elems.length, rangedMap.count());
        rangedMap.put(14l, new Element(14));
        assertEquals(elems.length + 1, rangedMap.count());
    }

    @Test
    public void testForEach() {
        testPut();
        rangedMap.forEach((Element e) -> {
            e.n += 5;
        });
        for (int i = 0; i < elems.length; i++) {
            assertEquals(i + 5, elems[i].n);
        }
    }

    @Test
    public void testForEachBiConsumer() {
        testPut();
        rangedMap.forEach((Long i, Element e) -> {
            e.n += i;
        });
        for (int i = 0; i < elems.length; i++) {
            assertEquals(i + i, elems[i].n);
        }
    }

    @Test
    public void testForEachWithRange() {
        testPut();
        rangedMap.forEach(new LongRange(2, 5), (Element e) -> {
            e.n += 5;
        });
        for (int i = 0; i < elems.length; i++) {
            if (i < 2 || i >= 5) {
                assertEquals(i, elems[i].n);
            } else {
                assertEquals(i + 5, elems[i].n);
            }
        }
    }

    @Test
    public void testForEachWithRangeBiConsumer() {
        testPut();
        rangedMap.forEach(new LongRange(2, 5), (Long i, Element e) -> {
            e.n += i;
        });
        for (int i = 0; i < elems.length; i++) {
            if (i < 2 || i >= 5) {
                assertEquals(i, elems[i].n);
            } else {
                assertEquals(i + i, elems[i].n);
            }
        }
    }

    @Test
    public void testParallelForEach() {
        testPut();
        rangedMap.parallelForEach((Element e) -> {
            e.n += 5;
        });
        for (int i = 0; i < elems.length; i++) {
            assertEquals(i + 5, elems[i].n);
        }
    }

    @Test
    public void testPut() {
        for (final LongRange r : ranges) {
            for (final long j : r) {
                rangedMap.put(j, elems[(int) j]);
            }
        }
    }

    @Test
    public void testRemove() {
        assertNull(rangedMap.remove(0l));
        testPut();
        assertEquals(elems[0], rangedMap.remove(0l));
        assertEquals(mapSize - 1, rangedMap.count());
    }

    @Test
    public void testRemoveWithRange() {
        testPut();
        rangedMap.remove(ranges[0]);
        assertEquals(mapSize - ranges[0].size(), rangedMap.size());
        assertEquals(mapSize - ranges[0].size(), rangedMap.count());
        rangedMap.remove(new LongRange(-100, 100));
        assertEquals(mapSize - ranges[0].size(), rangedMap.size());
        assertEquals(mapSize - ranges[0].size(), rangedMap.count());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetWithException() {
        rangedMap.put(6l, new Element(-1));
    }

    @Test
    public void testSize() {
        assertEquals(mapSize, rangedMap.size());
        rangedMap.addRange(new LongRange(10, 15));
        assertEquals(mapSize + 5, rangedMap.size());
    }

    @Test
    public void testSplit() {
        testPut();
        for (int i = 0; i < ranges.length; i++) {
            final RangedMap<Element> split = rangedMap.split(ranges[i]);
            assertEquals(ranges[i].size(), split.count());
            assertEquals(ranges.length, rangedMap.ranges().size());
        }
        RangedMap<Element> split = rangedMap.split(new LongRange(1, 4));
        assertEquals(5, rangedMap.ranges().size());
        assertEquals(2, split.ranges().size());
        long i = 1l;
        for (final Entry<Long, Element> e : split.entrySet()) {
            assertEquals(i, (long) e.getKey());
            i++;
        }
        split = rangedMap.split(new LongRange(-1, 2));
        assertEquals(6, rangedMap.ranges().size());
        assertEquals(2, split.ranges().size());
    }

}
