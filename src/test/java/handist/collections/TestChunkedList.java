package handist.collections;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestChunkedList {

	public class Element {
		public int n = 0;
		public Element(int i) {
			n = i;
		}
		public Element increase(int i) {
			n += i;
			return this;
		}
		@Override
		public String toString() {
			return String.valueOf(n);
		}
	}

	/** chunkedList filled with some initial members */
	ChunkedList<Element> chunkedList;
	/** Contains 3 different Chunk instances in an array */
	@SuppressWarnings("unchecked")
	Chunk<Element>[] chunks = new Chunk[3];

	/** Contains 6 initialized instances of class Element */
	Element[] elems = new Element[6];
	/** freshly created chunkedList which is empty */
	ChunkedList<Element> newlyCreatedChunkedList;


	@Before
	public void setUp() throws Exception {
		newlyCreatedChunkedList = new ChunkedList<>();

		Chunk<Element> firstChunk = new Chunk<>(new LongRange(0, 3)); 
		Chunk<Element> secondChunk = new Chunk<>(new LongRange(3,5));
		Chunk<Element> thirdChunk = new Chunk<>(new LongRange(5,6));
		chunks[0] = firstChunk;
		chunks[1] = secondChunk;
		chunks[2] = thirdChunk;

		for(int i = 0; i < 6; i++) { 
			elems[i] = new Element(i);
		}
		for(int i = 0; i < 3; i++) { 
			chunks[0].set(i, elems[i]);
		}
		for(int i = 3; i < 5; i++) {
			chunks[1].set(i, elems[i]);
		}
		chunks[2].set(5, elems[5]);

		chunkedList = new ChunkedList<>();
		chunkedList.addChunk(chunks[0]);
		chunkedList.addChunk(chunks[1]);
		chunkedList.addChunk(chunks[2]);
		chunkedList.set(4, null);	// include value for a null test
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testAdd() {
		chunkedList.add(null);
	}

	@Test
	public void testAddChunk() {
		Chunk<Element> emptyChunk = new Chunk<>(new LongRange(0));
		newlyCreatedChunkedList.addChunk(emptyChunk);
		assertEquals(0l, newlyCreatedChunkedList.longSize());
		assertEquals(1, newlyCreatedChunkedList.numChunks());

		newlyCreatedChunkedList.addChunk(chunks[1]);
		assertEquals(2l, newlyCreatedChunkedList.longSize());
		assertEquals(2, newlyCreatedChunkedList.numChunks());
	}

	@Test(expected = RuntimeException.class)
	public void testAddChunkErrorIdenticalChunk() {
		chunkedList.addChunk(chunks[1]);
	}
	
	@Test(expected = RuntimeException.class)
	public void testAddChunkErrorOverlapChunk1() {
		chunkedList.addChunk(new Chunk<>(new LongRange(0)));
	}
	
	@Test(expected = RuntimeException.class)
	public void testAddChunkErrorOverlapChunk2() {
		chunkedList.addChunk(new Chunk<>(new LongRange(2,3)));
	}
	
	@Test(expected = RuntimeException.class)
	public void testAddChunkErrorOverlapChunk3() {
		chunkedList.addChunk(new Chunk<>(new LongRange(3,4)));
	}
	
	@Test(expected = RuntimeException.class)
	public void testAddChunkErrorOverlapChunk4() {
		chunkedList.addChunk(new Chunk<>(new LongRange(-1,7)));
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testAddAll() {
		chunkedList.addAll(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testClear() {
		chunkedList.clear();
	}

	@Test
	public void testContains() {
		assertTrue(chunkedList.contains(elems[0]));
		assertFalse(chunkedList.contains(new Element(0)));	
		assertTrue(chunkedList.contains(null));
	}

	@Test
	public void testContainsAll() {
		ArrayList<Element> aList1 = new ArrayList<>(Arrays.asList(elems[0], elems[1]));
		assertTrue(chunkedList.containsAll(aList1));

		ArrayList<Element> aList2 = new ArrayList<>(Arrays.asList(new Element(0), new Element(1)));
		assertFalse(chunkedList.containsAll(aList2));

		ArrayList<Element> aList3 = new ArrayList<>();
		assertTrue(chunkedList.containsAll(aList3));
	}

	@Test
	public void testContainsChunk() {
		assertTrue(chunkedList.containsChunk(chunks[0]));
		assertFalse(chunkedList.containsChunk(new Chunk<Element>(new LongRange(6, 9))));
		assertFalse(chunkedList.containsChunk(null));	
	}

	//containsIndex, contains, containsAll, containsChunk
	@Test
	public void testContainsIndex() {
		assertTrue(chunkedList.containsIndex(0));
		assertTrue(chunkedList.containsIndex(5));
		assertFalse(chunkedList.containsIndex(100));
		assertFalse(chunkedList.containsIndex(-1));
	}

	@Test
	public void testFilterChunk() {
		List<RangedList<Element>> l = chunkedList.filterChunk(chunk -> chunk.isEmpty());
		assertSame(l.size(), 0);

		l = chunkedList.filterChunk(chunk -> chunk == chunks[0]);
		assertSame(l.size(), 1);
	}

	@Test
	public void testForEach() {
		// Remove the null value
		chunkedList.set(4l, elems[4]);

		// Keep the original values of the elements on the side
		int [] originalValues = new int[elems.length];
		for (int i = 0; i < elems.length; i ++) {
			originalValues[i] = elems[i].n;
		}

		// Increase each element by 2
		chunkedList.forEach((e) -> e.increase(2));

		// Check that each element has its value increased by 2
		for (int i = 0; i < elems.length; i++) {
			assertEquals(originalValues[i]+2, chunkedList.get(i).n);
		}
	}


	@Test(expected=NullPointerException.class)
	public void testForEachWithNull() {
		chunkedList.forEach((e) -> e.increase(2));
	}

	@Test
	public void testGet() {
		assertEquals(elems[0], chunkedList.get(0));
		assertEquals(elems[3], chunkedList.get(3));
		assertEquals(elems[5], chunkedList.get(5));

	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testGetError() {
		chunkedList.get(6);
	}

	@Test
	public void testIsEmpty() {
		assertFalse(chunkedList.isEmpty());
		assertTrue(newlyCreatedChunkedList.isEmpty());
	}

	@Test
	public void testLongSize() {
		assertEquals(6l, chunkedList.longSize());
		assertEquals(0, newlyCreatedChunkedList.longSize());
	}


	@Test
	public void testMap() {
		// First, remove the null element
		chunkedList.set(4, new Element(42));
		ChunkedList<Integer> integerChunkedList = chunkedList.map(e-> e.n + 5);

		// Check that the returned ChunkedList and the original have 
		// the same number of elements and that the values held by the 
		// result are indeed the ones we expect
		assertNotSame(chunkedList, integerChunkedList);
		assertEquals(6, chunkedList.longSize());
		assertSame(chunkedList.longSize(), integerChunkedList.longSize());
		Iterator<Element> originalIterator = chunkedList.iterator();
		Iterator<Integer> integerIterator = integerChunkedList.iterator();
		while (integerIterator.hasNext()) {
			assertSame(integerIterator.next(), originalIterator.next().n + 5);
		}
	}


	@Test(expected = NullPointerException.class)
	public void testMapWithNullElement() {		
		chunkedList.map(e -> e.increase(5));
	}


	@Test
	public void testNumChunks() {
		assertSame(chunkedList.numChunks(), 3);

		chunkedList.addChunk(new Chunk<Element>(new LongRange(6, 9)));
		assertSame(chunkedList.numChunks(), 4);

		chunkedList = new ChunkedList<Element>();
		assertSame(chunkedList.numChunks(), 0);
	}

	@Test
	public void testRanges() {
		int i = 0;
		for(LongRange range: chunkedList.ranges()) {
			assertEquals(range, chunks[i].getRange());
			i++;
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
		chunkedList.remove(null);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRemoveAll() {
		chunkedList.removeAll(null);
	}

	@Test
	public void testRemoveChunk() {
		Chunk<Element> chunkToRemove = new Chunk<>(new LongRange(-1l, 0l));
		// Removes nothing, the indices do not intersect. 
		RangedList<Element> removed = chunkedList.removeChunk(chunkToRemove);
		assertEquals(6, chunkedList.size());
		assertNull(removed);

		// A LongRange of same included and excluded bounds not present in the
		// chunked list
		chunkToRemove = new Chunk<>(new LongRange(0l));
		removed = chunkedList.removeChunk(chunkToRemove);
		assertEquals(6, chunkedList.size());

		// A Chunk that is included but not identical to a chunk of the
		// chunked list is not removed
		chunkToRemove = new Chunk<>(new LongRange(0l, 1l));
		chunkedList.removeChunk(chunkToRemove);
		assertEquals(6, chunkedList.size());

		// A chunk with the same range will be removed
		chunkToRemove = new Chunk<>(new LongRange(0l, 3l));
		removed = chunkedList.removeChunk(chunkToRemove);
		assertEquals(3, chunkedList.size());
		assertEquals(3l, removed.longSize());
		for (Element e : removed) {
			assertTrue(removed.contains(e));
			assertFalse(chunkedList.contains(e));
		}

		// If the same object is given as parameter, also works
		removed = chunkedList.removeChunk(chunks[1]);
		assertEquals(1, chunkedList.size());
		assertEquals(2l, removed.longSize());
		for (Element e : removed) {
			assertTrue(removed.contains(e));
			assertFalse(chunkedList.contains(e));
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testRetainAll() {
		chunkedList.retainAll(null);
	}

	@Test
	public void testSeparate() {
		List<ChunkedList<Element>> cLists = chunkedList.separate(2);
		assertSame(cLists.size(), 2);

		assertSame(cLists.get(0).size(), 3);
		assertSame(cLists.get(1).size(), 3);

		assertEquals(cLists.get(0).get(0), elems[0]);
		assertEquals(cLists.get(1).get(5), elems[5]);

		cLists.clear();
		cLists = chunkedList.separate(4);
		assertSame(4, cLists.size());

		assertSame(2, cLists.get(0).size());
		assertSame(2, cLists.get(1).size());
		assertSame(1, cLists.get(2).size());
		assertSame(1, cLists.get(3).size());

		assertEquals(cLists.get(0).get(0), elems[0]);
		assertEquals(cLists.get(1).get(2), elems[2]);
		// Manually inserted null value
		assertEquals(cLists.get(2).get(4), null);
		assertEquals(cLists.get(3).get(5), elems[5]);

		cLists = chunkedList.separate((10));
		assertSame(cLists.size(), 10);
		assertSame(cLists.get(5).size(), 1);
		assertSame(cLists.get(9).size(), 0);
	}

	@Test
	public void testSet() {
		Element e = new Element(-1);
		chunkedList.set(0, e);
		assertEquals(chunkedList.get(0), e);
	}

	@Test(expected = IndexOutOfBoundsException.class)
	public void testSetError() {
		chunkedList.set(6, new Element(-1));
	}

	@Test
	public void testSize() {
		assertEquals(6, chunkedList.size());	
		assertEquals(0, newlyCreatedChunkedList.size());
	}


	@Test(expected = UnsupportedOperationException.class)
	public void testToArray() {
		chunkedList.toArray();
	}


	@Test(expected = UnsupportedOperationException.class)
	public void testToArrayWithParameters() {
		chunkedList.toArray(new Object[2]);
	}
}
