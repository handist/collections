package handist.collections.util;

/**
 * Class containing features not present in the Standard Java Library
 */
public class StringUtilities {

    /**
     * Compares the two {@link String}s given as parameters and returns true if they
     * are equal or both {@code null}.
     *
     * @param s1 first {@link String}
     * @param s2 second {@link String}
     * @return true iff both strings are {@code null} or they are identical
     */
    public static boolean nullSafeEquals(String s1, String s2) {
        if (s1 == null) {
            return s2 == null;
        }
        return s1.equals(s2);
    }
}
