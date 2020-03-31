package handist.util;

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
    public final long begin; // INCLUSIVE
    public final long end; // EXCLUSIVE

    /**
     * construct a LongRange from begin (inclusive) to end (exclusive) 
     * @param begin
     * @param end
     */ 
    
     public LongRange(long begin, long end) {
        this.begin = begin;
        this.end = end;
    }
    
    /**
     * construct a empty LongRange from begin to begin.
     * Mainly used for comparation or search.
     * @param begin
     */
    public LongRange(long begin) {
        this.begin = this.end = begin;
    }

    public long size() {
        return end - begin;
    }

    public boolean contains(long index) {
        return (begin <= index) && (index < end);
    }

    public boolean contains(LongRange range) {
        return (this.begin <= range.begin) && (range.end <= this.end);
    }

    public boolean isOverlapped(LongRange range) {
        if (begin == end || range.begin == range.end) {
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

    public void forEach(LongConsumer func) {
        for (long current = begin; current < end; current++) {
            func.accept(current);
        }
    }

    public LongStream stream() {
        return LongStream.range(this.begin, this.end);
        // return LongStream.rangeClosed(this.min, this.max);
    }

    public int compareTo(LongRange o) {
        if (o == null) {
            throw new NullPointerException();
        }

        // if (o instanceof LongRange) {
        LongRange r = ((LongRange) o);
        if (r.begin <= this.begin && this.end <= r.end) {
            return 0;
        } else if (this.end <= r.begin) {
            return -1;
        } else if (r.end <= this.begin) {
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
        return "[" + this.begin + "," + this.end + ")";
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
	if(!(o instanceof LongRange)) return false;
	LongRange range2 = (LongRange)o;
	return this.begin==range2.begin && this.end==range2.end;
    }

    @Override
    public int hashCode() {
	return (int)((begin << 4) + (begin>>16) + end);
    }

    class It implements Iterator<Long> {
        long current;

        It() {
            current = begin;
        }

        @Override
        public boolean hasNext() {
            return current < end;
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
        long c = this.begin;

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
                    long from = c.begin + used;
                    r.add(new LongRange(from, c.end));
                    rest -= c.size() - used;
                    used = 0;
                    if (!iter.hasNext())
                        throw new Error("Should not happen!");
		            c = iter.next();
                } else {
                    long from = c.begin + used;
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
