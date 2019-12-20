package handist.util;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;

public class LongRange implements Comparable {
    public final long begin;  // INCLUSIVE
    public final long end;    // EXCLUSIVE

    public LongRange(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }

    public LongRange(long index) {
        this.begin = index;
        this.end = index;
    }

    public boolean contains(long index) {
	return (begin <= index) && (index < end);
    }

    public boolean isOverlapped(LongRange range) {
	if (begin == end ||
	    range.begin == range.end) {
	    return false;
	}
	if (begin < range.begin) {
	    if (range.begin < end) {
		return true;
	    } else {
		return false;
	    }
	} else {
	    if (begin < range.end) {
		return true;
	    } else {
		return false;
	    }
	}
    }

    public LongStream stream() {
        return LongStream.range(this.begin, this.end);
//        return LongStream.rangeClosed(this.min, this.max);
    }

    @Override
    public int compareTo(Object o) {
        if (o == null) {
            throw new NullPointerException();
        }

        if (o instanceof LongRange) {
            LongRange r = ((LongRange) o);
            if (r.begin <= this.begin && this.end <= r.end) {
                return 0;
            } else if (this.end <= r.begin) {
                return -1;
            } else if (r.end <= this.begin) {
                return 1;
            }
//            return Integer.compare(this.begin, r.begin);
        }
//        else if (o instanceof Integer) { // this doesn't work
//            Integer i = ((Integer) o);
//            if (this.begin <= i && i < this.end) return 0;
//            else if (i < this.begin) return -1;
//            else if (this.end <= i) return 1;
//        }

        throw new ClassCastException();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof LongRange) {
	    LongRange r = ((LongRange) o);
	    if (r.begin <= this.begin && this.end <= r.end) {
		return true;
	    } else {
		return false;
	    }
        } else if (o instanceof Long) {
            Long i = ((Long) o);
            return this.begin <= i && i < this.end;
        }
        return false;
    }

    @Override
    public String toString() {
	return "" + this.begin + ".." + (this.end - 1);
    }

    public static void main(String[] args) {
        Map<LongRange, String> m = new TreeMap<>();
        m.put(new LongRange(0, 3), "(0,3]");
        m.put(new LongRange(5, 10), "(5,10]");
        m.put(new LongRange(3, 5), "(3,5]");

        System.out.println(m.containsKey(new LongRange(1, 1)));
        System.out.println(m.get(new LongRange(1, 1)));

//
//        System.out.println(m);
//        LongRange range = new LongRange(0, 3);
//        System.out.println(range.compareTo(1));
//        System.out.println(range.compareTo(-1));
//        System.out.println(range.compareTo(5));

//        System.out.println(m.containsKey(1));
//        System.out.println(m.get(new LongRange(0,3)));
//
//        System.out.println(m.get(5));
    }
}
