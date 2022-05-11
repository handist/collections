package handist.collections.util;

import java.util.List;

public class Splitter {
    public final int from;
    public final int total;
    public final int attendance;
    public final int div;
    public final int rem;

    public Splitter(int total, int attendance) {
        this.from = 0;
        this.total = total;
        this.attendance = attendance;
        this.div = total / attendance;
        this.rem = total % attendance;
    }

    public Splitter(int from, int to, int attendance) {
        this.from = from;
        this.total = to - from;
        this.attendance = attendance;
        this.div = total / attendance;
        this.rem = total % attendance;
    }

    public <E> List<E> getIth(int i, List<E> src) {
        return src.subList(ith(i), ith(i + 1));
    }

    public int ith(int i) {
        return from + div * i + Math.min(i, rem);
    }
}
