package handist.collections;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

public class TestParallelMap {

    /**
     * Class used as element in the tests of the distributed collections
     */
    public class Element implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = -2659467143487621997L;

        /** String contained in the element */
        String s;

        /**
         * Constructor with initial String value
         *
         * @param string initial value of the contained string, may be null
         */
        Element(String string) {
            s = string;
        }
    }

    /** Size of the dataset used for the tests **/
    public static final long numData = 200;

    /** Random object used to generate values */
    static Random random = new Random(12345l);

    /**
     * Helper method to generate Strings with the provided prefix.
     *
     * @param prefix the String prefix of the Random string generated
     * @return a random String with the provided prefix
     */
    public static String genRandStr(String prefix) {
        final long rndLong = random.nextLong();
        return prefix + rndLong;
    }

    private ParallelMap<String, Element> map;

    @Before
    public void setup() {
        map = new ParallelMap<>();
        for (int i = 0; i < numData; i++) {
            map.put(genRandStr("k"), new Element(genRandStr("v")));
        }
    }

    @Test(timeout = 5000)
    public void testParallelForEachKeyValue() {
        map.parallelForEach((k, e) -> {
            e.s = e.s + "K" + k;
        });

        // Check that every element contains K
        for (final String key : map.keySet()) {
            final Element e = map.get(key);
            assertTrue(e.s.contains("K"));
            assertTrue(e.s.contains(key));
        }
    }

    @Test(timeout = 5000)
    public void testParallelForEachValue() {
        map.parallelForEach(e -> {
            e.s = e.s + "K";
        });

        // Check that every element contains K
        for (final Element e : map.values()) {
            assertTrue(e.s.contains("K"));
        }
    }

    @Test(timeout = 5000)
    public void testReduceLocal() {
        final int size = map.reduceLocal((Integer sum, Element v) -> {
            return sum += 1;
        }, 0);
        assertEquals(numData, size);
    }

    @Test(timeout = 5000)
    public void testSetProxyGenerator() {
        map.setProxyGenerator((key) -> {
            return new Element("PG");
        });
        assertEquals("PG", map.get("test").s);
        assertFalse(map.containsKey("test"));
    }

}
