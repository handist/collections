package handist.collections.glb.lifeline;

import static org.junit.Assert.*;

import apgas.Place;

/**
 * Provides some static helper methods used to test Lifeline implementations
 *
 * @author Patrick Finnerty
 *
 */
public final class LifelineTestHelper {
    /**
     * Checks that every "reverse lifeline" declared has indeed a lifeline to match
     * it
     *
     * @param l the lifeline implementation to check
     */
    public static void checkLifeline(Lifeline l) {
        for (final Place p : l.sortedListOfPlaces) {
            for (final Place origin : l.reverseLifeline(p)) {
                assertTrue(l.lifeline(origin).contains(p));
            }
        }
    }

    /**
     * Checks that every "lifeline" declared has indeed a reverse lifeline to match
     * it
     *
     * @param l the lifeline implementation to check
     */
    public static void checkReverseLifeline(Lifeline l) {
        for (final Place p : l.sortedListOfPlaces) {
            for (final Place origin : l.lifeline(p)) {
                assertTrue(l.reverseLifeline(origin).contains(p));
            }
        }
    }
}
