package handist.collections;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import handist.collections.function.LTConsumer;

public class ChunkedList<T> extends AbstractCollection<T> {

	// private List<RangedList<T>> chunks;
	private TreeMap<LongRange, RangedList<T>> chunks;
	private long size = 0;

	public ChunkedList() {
		//chunks = new TreeMap<>(Comparator.comparingLong(r -> r.from));
		chunks = new TreeMap<LongRange, RangedList<T>>();
	}

	public ChunkedList(TreeMap<LongRange, RangedList<T>> chunks) {
		this.chunks = chunks;
	}

	public void checkOverlap(LongRange range) {
		LongRange floorKey = chunks.floorKey(range);
		if (floorKey != null && floorKey.isOverlapped(range)) {
			throw new IllegalArgumentException("ChunkedList#checkOverlap : requested range " + range
					+ " is overlapped with " + chunks.get(floorKey));
		}

		LongRange nextKey = chunks.higherKey(range);
		if (nextKey != null && nextKey.isOverlapped(range)) {
			throw new IllegalArgumentException("ChunkedList#checkOverlap : requested range " + range
					+ " is overlapped with " + chunks.get(nextKey));
		}
	}

	public boolean containsIndex(long i) {
		LongRange r = new LongRange(i);
		Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
		if (entry == null || !entry.getKey().contains(i)) {
			entry = chunks.ceilingEntry(r);
			if (entry == null || !entry.getKey().contains(i)) {
				return false;
			}
		}
		return true;
	}

	public T get(long i) {
		LongRange r = new LongRange(i);
		Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
		if (entry == null || !entry.getKey().contains(i)) {
			entry = chunks.ceilingEntry(r);
			if (entry == null || !entry.getKey().contains(i)) {
				throw new IndexOutOfBoundsException("ChunkedList: index " + i + " is out of range of " + chunks);
			}
		}
		RangedList<T> chunk = entry.getValue();
		return chunk.get(i);
	}

	public T set(long i, T value) {
		LongRange r = new LongRange(i);
		Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(r);
		if (entry == null || !entry.getKey().contains(i)) {
			entry = chunks.ceilingEntry(r);
			if (entry == null || !entry.getKey().contains(i)) {
				throw new IndexOutOfBoundsException("ChunkedList: index " + i + " is out of range of " + chunks);
			}
		}
		RangedList<T> chunk = entry.getValue();
		return chunk.set(i, value);
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Clear the local elements
	 */
	@Override
	public void clear() {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean contains(Object o) {
		for (RangedList<T> chunk : chunks.values()) {
			if (chunk.contains(o)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// cf.
		// https://stackoverflow.com/questions/10199772/what-is-the-cost-of-containsall-in-java
		Iterator<?> e = c.iterator();
		while (e.hasNext()) {
			if (!this.contains(e.next())) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Return whether this chunked list contins the given chunk.
	 */

	public boolean containsChunk(RangedList<T> c) {
		return chunks.containsValue(c);
	}

	/**
	 * Return the number of local elements.
	 *
	 * @return the number of local elements.
	 */
	@Override
	public int size() {
		return (int) longSize();
	}

	public long longSize() {
		return size;
	}

	/**
	 * Return a Container that has the same values in the DistCol.
	 *
	 * @return a Container that has the same values in the DistCol.
	 */
	@Override
	protected Object clone() {
		TreeMap<LongRange, RangedList<T>> newChunks = new TreeMap<>();
		for (RangedList<T> c : chunks.values()) {
			newChunks.put(c.getRange(), ((Chunk<T>) c).clone());
		}
		return new ChunkedList<T>(newChunks);
	}

	/**
	 * Return the logical range assined end local.
	 *
	 * @return an instance of LongRange.
	 */
	public Collection<LongRange> ranges() {
		return chunks.keySet();
	}

	public void addChunk(RangedList<T> c) {
		checkOverlap(c.getRange());
		chunks.put(c.getRange(), c);
		size += c.longSize();
	}

	public RangedList<T> removeChunk(RangedList<T> c) {
		RangedList<T> removed = chunks.remove(c.getRange());
		if (removed != null) {
			size -=removed.longSize();
		}
		return removed;
	}

	public int numChunks() {
		return chunks.size();
	}

	public void forEach(Consumer<? super T> action) {
		for (RangedList<T> c : chunks.values()) {
			c.forEach(action);
		}
	}

	public void forEach(LTConsumer<? super T> action) {
		for (RangedList<T> c : chunks.values()) {
			c.forEach(c.getRange(), action);
		}
	}

	public <U> void forEach(BiConsumer<? super T, Consumer<U>> action, Consumer<U> receiver) {
		for (RangedList<T> c : chunks.values()) {
			c.forEach(t -> action.accept((T) t, receiver));
		}
	}

	public <U> void forEach(BiConsumer<? super T, Consumer<U>> action, final Collection<? super U> toStore) {
		forEach(action, new Consumer<U>() {
			@Override
			public void accept(U u) {
				toStore.add(u);
			}
		});
	}

	private List<Future<?>> forEachParallelBody(ExecutorService pool, int nthreads, Consumer<ChunkedList<T>> run) {
		List<ChunkedList<T>> separated = this.separate(nthreads);
		List<Future<?>> futures = new ArrayList<>();
		for (ChunkedList<T> sub: separated) {
			futures.add(pool.submit(() -> {
				run.accept(sub);
			}));
		}
		return futures;
	}

	private void waitNfutures(List<Future<?>> futures) {
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				throw new RuntimeException("[ChunkedList] exception raised by worker threads.");
			}
		}
	}
	public void forEach(ExecutorService pool, int nthreads, Consumer<? super T> action) {
		List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
			sub.forEach(action);
		});
		waitNfutures(futures);
	}

	public Future<ChunkedList<T>> asyncforEach(ExecutorService pool, int nthreads, Consumer<? super T> action) {
		List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
			sub.forEach(action);
		});
		return new FutureN.ReturnGivenResult<ChunkedList<T>>(futures,  this);
	}

	public void forEach(ExecutorService pool, int nthreads, LTConsumer<? super T> action) {
		List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
			sub.forEach(action);
		});
		waitNfutures(futures);
	}

	public Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads, LTConsumer<? super T> action) {
		List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
			sub.forEach(action);
		});
		return new FutureN.ReturnGivenResult<ChunkedList<T>>(futures, this);
	}

	public <U> void forEach(ExecutorService pool, int nthreads, BiConsumer<? super T, Consumer<U>> action,
			final MultiReceiver<U> toStore) {
		List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
			sub.forEach(action,toStore.getReceiver());
		});
		waitNfutures(futures);
	}
	public <U> Future<ChunkedList<T>> asyncForEach(ExecutorService pool, int nthreads, BiConsumer<? super T, Consumer<U>> action,
			final MultiReceiver<U> toStore) {
		List<Future<?>> futures = forEachParallelBody(pool, nthreads, (ChunkedList<T> sub) -> {
			sub.forEach(action,toStore.getReceiver());
		});
		return new FutureN.ReturnGivenResult<ChunkedList<T>>(futures, this);
	}



	public <S> ChunkedList<S> map(Function<? super T, ? extends S> func) {
		ChunkedList<S> result = new ChunkedList<>();
		forEachChunk((RangedList<T> c) -> {
			RangedList<S> r = c.map(func);
			result.addChunk(r);
		});
		return result;
	}

	private <S> void mapTo(ChunkedList<S> to, Function<? super T, ? extends S> func) {
		Iterator<RangedList<T>> fromIter = chunks.values().iterator();
		Iterator<RangedList<S>> toIter = to.chunks.values().iterator();
		while (fromIter.hasNext()) {
			assert (toIter.hasNext());
			RangedList<T> fromChunk = fromIter.next();
			RangedList<S> toChunk = toIter.next();
			toChunk.setupFrom(fromChunk, func);
		}
	}

	private <S> List<Future<?>> mapParallelBody(ExecutorService pool, int nthreads,
			Function<? super T, ? extends S> func, ChunkedList<S> result) {
		forEachChunk((RangedList<T> c) -> {
			result.addChunk(new Chunk<S>(c.getRange()));
		});
		List<ChunkedList<T>> separatedIn = this.separate(nthreads);
		List<ChunkedList<S>> separatedOut = result.separate(nthreads);
		List<Future<?>> futures = new ArrayList<>();
		for (int i = 0; i < nthreads; i++) {
			final int i0 = i;
			futures.add(pool.submit(() -> {
				ChunkedList<T> from = separatedIn.get(i0);
				ChunkedList<S> to = separatedOut.get(i0);
				from.mapTo(to, func);
				return null;
			}));
		}
		return futures;
	}

	public <S> ChunkedList<S> map(ExecutorService pool, int nthreads, Function<? super T, ? extends S> func) {
		ChunkedList<S> result = new ChunkedList<>();
		List<Future<?>> futures = mapParallelBody(pool, nthreads, func, result);
		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				throw new RuntimeException("[ChunkedList] exception raised by worker threads.");
			}
		}
		return result;
	}

	public <S> Future<ChunkedList<S>> asyncMap(ExecutorService pool, int nthreads,
			Function<? super T, ? extends S> func) {
		final ChunkedList<S> result = new ChunkedList<>();
		final List<Future<?>> futures = mapParallelBody(pool, nthreads, func, result);

		for (Future<?> f : futures) {
			try {
				f.get();
			} catch (InterruptedException | ExecutionException e) {
				e.printStackTrace();
				throw new RuntimeException("[ChunkedList] exception raised by worker threads.");
			}
		}
		return new FutureN.ReturnGivenResult<ChunkedList<S>>(futures, result);
	}

	public void forEachChunk(Consumer<RangedList<T>> op) {
		for (RangedList<T> c : chunks.values()) {
			op.accept(c);
		}
	}

	/*
    public Map<LongRange, RangedList<T>> filterChunk0(Predicate<RangedList<? super T>> filter) {
        TreeMap<LongRange, RangedList<T>> map = new TreeMap<>();
        for (RangedList<T> c : chunks.values()) {
            if (filter.test(c)) {
                map.put(c.getRange(), c);
            }
        }
        return map;
    }*/
	public List<RangedList<T>> filterChunk(Predicate<RangedList<? super T>> filter) {
		List<RangedList<T>> result = new ArrayList<>();
		for (RangedList<T> c : chunks.values()) {
			if (filter.test(c)) {
				result.add(c);
			}
		}
		return result;
	}

	/**
	 * Seperates the contents of the ChunkedList in <em>n</em> parts.
	 * This can be used to apply a forEach method in parallel using 'n' threads
	 * for instance. The method returns <em>n</em> lists, each containing a
	 * {@link ChunkedList} of <em>T</em>s.
	 *
	 * @param n the number of parts in which to split the ChunkedList
	 * @return <em>n</em> {@link ChunkedList}s containing the same number of
	 * 	elements
	 */
	public List<ChunkedList<T>> separate(int n) {
		long totalNum = size();
		long rem = totalNum % n;
		long quo = totalNum / n;
		List<ChunkedList<T>> result = new ArrayList<ChunkedList<T>>(n);
		RangedList<T> c = chunks.firstEntry().getValue();
		long used = 0;

		for (int i = 0; i < n; i++) {
			ChunkedList<T> r = new ChunkedList<>();
			result.add(r);
			long rest = quo + ((i < rem) ? 1 : 0);
			while (rest > 0) {
				LongRange range = c.getRange();
				if (c.longSize() - used < rest) { // not enough
					long from = range.from + used;
					r.addChunk(c.subList(from, range.to));
					rest -= c.longSize() - used;
					used = 0;
					// TODO should use iterator??
					c = chunks.higherEntry(range).getValue();
				} else {
					long from = range.from + used;
					long to = from + rest;
					r.addChunk(c.subList(from, to));
					used += rest;
					rest = 0;
				}

			}
		}
		return result;
	}

	private static class It<S> implements Iterator<S> {
		public TreeMap<LongRange, RangedList<S>> chunks;
		private LongRange range;
		private Iterator<S> cIter;

		public It(TreeMap<LongRange, RangedList<S>> chunks) {
			this.chunks = chunks;
			Map.Entry<LongRange, RangedList<S>> firstEntry = chunks.firstEntry();
			if (firstEntry != null) {
				RangedList<S> firstChunk = firstEntry.getValue();
				range = firstChunk.getRange();
				cIter = firstChunk.iterator();
			} else {
				range = null;
				cIter = null;
			}
		}

		@Override
		public boolean hasNext() {
			if (range == null) {
				return false;
			}
			if (cIter.hasNext()) {
				return true;
			}
			Map.Entry<LongRange, RangedList<S>> nextEntry = chunks.higherEntry(range);
			if (nextEntry == null) {
				range = null;
				cIter = null;
				return false;
			}
			range = nextEntry.getKey();
			cIter = nextEntry.getValue().iterator();
			return cIter.hasNext();
		}

		@Override
		public S next() {
			if (hasNext()) {
				return cIter.next();
			}
			throw new IndexOutOfBoundsException();
		}

	}

	@Override
	public Object[] toArray() {
		//        return new Object[0];
		throw new UnsupportedOperationException();
	}

	@Override
	public <T1> T1[] toArray(T1[] a) {
		//        return null;
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean add(T element) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Iterator<T> iterator() {
		return new It<T>(chunks);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("[ChunksList(" + chunks.size() + ")");
		for (RangedList<T> c : chunks.values()) {
			sb.append("," + c);
		}
		sb.append("]");
		return sb.toString();
	}

	public static void main(String[] args) {

		ChunkedList<String> cl5 = new ChunkedList<>();

		// Test1: Add Chunks

		System.out.println("Test1");
		for (int i = 0; i < 10; i++) {
			if (i % 2 == 1) {
				continue;
			}
			long begin = i * 5;
			long end = (i + 1) * 5;
			Chunk<String> c = new Chunk<>(new LongRange(begin, end));
			for (long index = begin; index < end; index++) {
				c.set(index, String.valueOf(index));
			}
			cl5.addChunk(c);
		}
		System.out.println(cl5.toString());

		// Test2: Iterate using each()

		System.out.println("Test2");
		StringBuilder sb2 = new StringBuilder();
		cl5.forEach(value -> sb2.append(value + ","));
		System.out.println(sb2.toString());

		// Test3: Iterate using iterator()

		System.out.println("Test3");
		StringBuilder sb3 = new StringBuilder();
		Iterator<String> it = cl5.iterator();
		while (it.hasNext()) {
			sb3.append(it.next() + ",");
		}
		System.out.println(sb3.toString());

		// Test4: Raise exception

		System.out.println("Test4");
		for (int i = 0; i < 10; i++) {

			long begin = i * 5 - 1;
			long end = i * 5 + 1;
			Chunk<String> c = new Chunk<>(new LongRange(begin, end));
			try {
				cl5.addChunk(c);
				System.out.println("--- FAIL ---");
			} catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		for (int i = 0; i < 10; i++) {

			long begin = i * 5 - 1;
			long end = i * 5 + 5;
			Chunk<String> c = new Chunk<>(new LongRange(begin, end));
			try {
				cl5.addChunk(c);
				System.out.println("--- FAIL ---");
			} catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		for (int i = 0; i < 10; i++) {

			long begin = i * 5 - 1;
			long end = i * 5 + 10;
			Chunk<String> c = new Chunk<>(new LongRange(begin, end));
			try {
				cl5.addChunk(c);
				System.out.println("--- FAIL ---");
			} catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		System.out.println("--- OK ---");
		// Test5: Add RangedListView

		System.out.println("Test5");
		Chunk<String> c0 = new Chunk<>(new LongRange(0, 10 * 5));
		for (long i = 0; i < 10 * 5; i++) {
			c0.set(i, String.valueOf(i));
		}
		for (int i = 0; i < 10; i++) {
			if (i % 2 == 0) {
				continue;
			}
			long begin = i * 5;
			long end = (i + 1) * 5;
			RangedList<String> rl = c0.subList(begin, end);
			cl5.addChunk(rl);
		}
		System.out.println(cl5.toString());

		// Test6: Iterate combination of Chunk and RangedListView
		System.out.println("Test6");
		StringBuilder sb6 = new StringBuilder();
		Iterator<String> it6 = cl5.iterator();
		while (it6.hasNext()) {
			sb6.append(it6.next() + ",");
		}
		System.out.println(sb6.toString());

		// Test7: Raise exception on ChunkedList with continuous range

		System.out.println("Test7");
		for (int i = 0; i < 10 * 5; i++) {
			long begin = i - 1;
			long end = i + 1;
			Chunk<String> c = new Chunk<>(new LongRange(begin, end));
			try {
				cl5.addChunk(c);
				System.out.println("--- FAIL --- " + begin + "," + end);
			} catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		for (int i = 0; i < 10 * 5; i++) {
			long begin = i - 1;
			long end = i + 6;
			Chunk<String> c = new Chunk<>(new LongRange(begin, end));
			try {
				cl5.addChunk(c);
				System.out.println("--- FAIL --- " + begin + "," + end);
			} catch (IllegalArgumentException e) {
				// do nothing
			}
		}
		System.out.println("--- OK ---");

	}

}
