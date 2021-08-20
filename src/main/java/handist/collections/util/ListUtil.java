package handist.collections.util;

import java.util.List;

public class ListUtil {
    public static <X> List<X> splitGet(List<X> src, int i, int n) {
        int div = src.size() /n;
        int rem = src.size() % n;
        int from = div * i + Math.min(rem, i);
        int to = div * (i+1) + Math.min(rem, i+1);
        return src.subList(from, to);
    }
    public static <X> List<X> splitGet2(List<X> src, int i, int n, int j, int m) {
        return splitGet(src, i*m+j, n*m);
    }
}
