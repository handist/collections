package handist.collections;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;
import java.io.Serializable;

public class LongRange implements Comparable<LongRange>, Iterable<Long>, Serializable {
    /**
     *
     */
    private static final long serialVersionUID = 6430187870603427655L;
    public final long from; // INCLUSIVE
    public final long to; // EXCLUSIVE

    /**
     * construct a LongRange from from (inclusive) to to (exclusive) 
     * @param from
     * @param to
     */ 
    
     public LongRange(long from, long to) {
        this.from = from;
        this.to = to;
    }
    
    /**
     * construct a empty LongRange from index to index.
     * Mainly used for comparation or search.
     * @param index
     */
    public LongRange(long index) {
        this.from = this.to = index;
    }

    public long size() {
        return to - from;
    }

    public boolean contains(long index) {
        return (from <= index) && (index < to);
    }

    public boolean contains(LongRange range) {
        return (this.from <= range.from) && (range.to <= this.to);
    }

    public boolean isOverlapped(LongRange range) {
        if (from == to || range.from == range.to) {
            return false;
        }
        if (from < range.from) {
            if (range.from < to) {
                return true;
            } else {
                return false;
            }
        } else {
            if (from < range.to) {
                return true;
            } else {
                return false;
            }
        }
    }

    public void forEach(LongConsumer func) {
        for (long current = from; current < to; current++) {
            func.accept(current);
        }
    }

    public LongStream stream() {
        return LongStream.range(this.from, this.to);
        // return LongStream.rangeClosed(this.min, this.max);
    }

    public int compareTo(LongRange r) {
        if (r == null) {
            throw new NullPointerException();
        }

        // if (o instanceof LongRange) {
        if (r.from <= this.from && this.to <= r.to) {
            return 0;
        } else if (this.to <= r.from) {
            return -1;
        } else if (r.to <= this.from) {
            return 1;
        }
        // return Integer.compare(this.begin, r.begin);
        // }
        // else if (o instanceof Integer) { // this doesn't work
        // Integer i = ((Integer) o);
        // if (this.begin <= i && i < this.end) return 0;
        // else if (i < this.begin) return -1;
        // else if (this.end <= i) return 1;
        // }

        throw new ClassCastException();
    }


    @Override
    public String toString() {
        return "[" + this.from + "," + this.to + ")";
    }

    public static void main(String[] args) {
        Map<LongRange, String> m = new TreeMap<>();
        m.put(new LongRange(0, 3), "[0,3)");
        m.put(new LongRange(5, 10), "[5,10)");
        m.put(new LongRange(3, 5), "[3,5)");

        System.out.println(m.containsKey(new LongRange(1, 1)));
        System.out.println(m.get(new LongRange(1, 1)));

        //
        // System.out.println(m);
        // LongRange range = new LongRange(0, 3);
        // System.out.println(range.compareTo(1));
        // System.out.println(range.compareTo(-1));
        // System.out.println(range.compareTo(5));

        // System.out.println(m.containsKey(1));
        // System.out.println(m.get(new LongRange(0,3)));
        //
        // System.out.println(m.get(5));
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof LongRange))
            return false;
        LongRange range2 = (LongRange) o;
        return this.from == range2.from && this.to == range2.to;
    }

    @Override
    public int hashCode() {
	return (int)((from << 4) + (from>>16) + to);
    }

    class It implements Iterator<Long> {
        long current;

        It() {
            current = from;
        }

        @Override
        public boolean hasNext() {
            return current < to;
        }

        @Override
        public Long next() {
            return current++;
        }
    }

    @Override
    public Iterator<Long> iterator() {
        return new It();
    }


    public List<LongRange> split(int n) {
        ArrayList<LongRange> result = new ArrayList<>();
        long rem = size() % n;
        long quo = size() / n;
        long c = this.from;

        for (int i = 0; i < n; i++) {
            long given = quo + ((i < rem) ? 1 : 0);
            result.add(new LongRange(c, c+given));
            c += given;
        }
        return result;
    }

    public static List<List<LongRange>> splitList(int n, List<LongRange> list) {
        long totalNum = 0;
        for (LongRange item : list) {
            totalNum += item.size();
        }
        long rem = totalNum % n;
        long quo = totalNum / n;
        List<List<LongRange>> result = new ArrayList<>(n);
        Iterator<LongRange> iter = list.iterator();
        LongRange c = iter.next();
        long used = 0;

        for (int i = 0; i < n; i++) {
            List<LongRange> r = new ArrayList<>();
            result.add(r);
            long rest = quo + ((i < rem) ? 1 : 0);
            while (rest > 0) {
                if (c.size() - used <= rest) {
                    long from = c.from + used;
                    r.add(new LongRange(from, c.to));
                    rest -= c.size() - used;
                    used = 0;
                    if (!iter.hasNext())
                        throw new Error("Should not happen!");
		            c = iter.next();
                } else {
                    long from = c.from + used;
                    long to = from + rest;
                    r.add(new LongRange(from, to));
                    used += rest;
                    rest = 0;
                }

            }
        }
        return result;
    }





}
