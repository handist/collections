package handist.collections.util;

import java.util.List;

public class Splitter {
    private final int from;
    private final int total;
    private final int attendance;
    private final int div;
    private final int rem;

    public Splitter(int total, int attendance) {
        this.from =0;
        this.total = total;
        this.attendance = attendance;
        this.div = total / attendance;
        this.rem = total % attendance;
    }
    public Splitter(int from, int to, int attendance) {
        this.from = from;
        this.total = to-from;
        this.attendance = attendance;
        this.div = total / attendance;
        this.rem = total % attendance;
    }
    public int ith(int i){
        return from + div * i + Math.min(i, rem);
    }
    public <E> List<E> getIth(int i, List<E> src) {
        return src.subList(ith(i), ith(i+1));
    }
}
