package handist.collections;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
	 * Constructs a LongRange with the provided parameters. 
	 * 
	 * @param from lower bound of the range (inclusive)
	 * @param to upper bound of the range (exclusive)
	 * @throws IllegalArgumentException if the lower bound is strictly superior
	 * 	to the upper bound
	 */ 
	public LongRange(long from, long to) {
		if (from > to) {
			throw new IllegalArgumentException("Cannot create LongRange from " +
					from + " to " + to);
		}
		this.from = from;
		this.to = to;
	}

	/**
	 * Constructs an empty LongRange using a single point for a bound.
	 * Mainly used for comparison or search.
	 * @param index the lower and upper bound of the LongRange to create. 
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
			if (from <= range.from) { 
				return (to >= range.from) ; 
			} else { 
				return (from <= range.to);
			}
		} else {
			return (from < range.from)? (to > range.from) : (from < range.to);
		}
	}

	public void forEach(LongConsumer func) {
		for (long current = from; current < to; current++) {
			func.accept(current);
		}
	}

	public LongStream stream() {
		return LongStream.range(this.from, this.to);
	}

	public int compareTo(LongRange r) {
		if (to <= r.from && from != to ) {
			return -1;
		} else if (r.to <= from && from != to) {
			return 1;
		} 
		// The LongRange instances overlap,
		// We order them based on "from" first and "to" second
		int fromComparison = Long.compare(from, r.from);
		return (fromComparison == 0) ? Long.compare(to, r.to) : fromComparison; 
	}


	@Override
	public String toString() {
		return "[" + this.from + "," + this.to + ")";
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

	/**
	 * Splits the LongRange into <em>n</em> LongRange instances of equal size
	 * (or near equal size if the size of this instance is not divisible by 
	 * <em>n</em>.
	 * 
	 * @param n the number of LongRange instance in which to split this instance
	 * @return a list of <em>n</em> consecutive LongRange instances
	 */
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

	/**
	 * Splits the {@link LongRange} provided in the list into <em>n</em> lists 
	 * of {@link LongRange} instances such that the accumulated size of each 
	 * list's {@link LongRange} are the same. 
	 * <p>
	 * To achieve this, {@link LongRange} instances may be split into several 
	 * instances that will placed in different lists.
	 * <p>
	 * The {@link LongRange} instances given as parameter are not  
	 *     
	 * @param n number of lists of equal sizes
	 * @param longRanges {@link LongRange} instances to distribute into the 
	 * 	lists 
	 * @return lists of {@link LongRange} instances of equivalent 
	 */
	public static List<List<LongRange>> splitList(int n, List<LongRange> longRanges) {
		long totalNum = 0;
		for (LongRange item : longRanges) {
			totalNum += item.size();
		}
		long rem = totalNum % n;
		long quo = totalNum / n;
		List<List<LongRange>> result = new ArrayList<>(n);
		Iterator<LongRange> iter = longRanges.iterator();
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
					if (!iter.hasNext()) {
						// Avoids calling iter.next when the last LongRange has
						// been used. Is necessary due to this "border"  case
						break;
					}
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
