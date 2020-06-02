package handist.collections;

import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.hazelcast.query.IndexAwarePredicate;

import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.RangedListView;
import handist.collections.TestChunkedList.Element;

public class TestChunk {

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
	
	/** chunk filled with 5 initial members */
	private Chunk<Element> chunk;
	/** chunk include null member in index 0*/
	private Chunk<Element> includeNullChunk;
	/** Contains 5 initialized instances of class Element */
	Element[] elems = new Element[5];
	/** freshly created chunk which is empty */
	Chunk<Element> newlyCreatedChunk;

	
	@Before
	public void setUp() throws Exception {
		newlyCreatedChunk = new Chunk<Element>();
		chunk = new Chunk(new LongRange(0, 5));
		includeNullChunk = new Chunk(new LongRange(0, 5));
		
		for(int i = 0; i < 5; i++) {
			elems[i] = new Element(i);
			chunk.set(i, elems[i]);
		}
		includeNullChunk.set(0, null);
		
	}
	
	@After
	public void tearDown() throws Exception {
		//chunk.clear();
	}

	
	@Test
	public void testSet() {
		Element newElement = new Element(100);
		assertEquals(chunk.get(0), elems[0]);
		chunk.set(0, newElement);
		assertEquals(chunk.get(0), newElement);
	}
	
	
	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testSetError() {
		chunk.set(6, new Element(0));
	}
	
	
	@Test
	public void testGet() {
		for(int i = 0; i < 5; i++) {
			assertEquals(chunk.get(i), elems[i]);
		}
		assertEquals(includeNullChunk.get(0), null);
	}
	
	
	@Test(expected = IndexOutOfBoundsException.class)
	public void testGetError() {
		chunk.get(6);
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testClear() {
		chunk.clear();
	}
	
	
	@Test
	public void testContains() {
		for(int i = 0; i < 5; i++) {
			assertTrue(chunk.contains(elems[i]));
		}
		assertFalse(chunk.contains(new Element(0)));
		assertFalse(chunk.contains(null));
		
		assertFalse(includeNullChunk.contains(new Element(0)));
		assertTrue(includeNullChunk.contains(null));
	}
	
	
	@Test
	public void testSize() {
		assertSame(chunk.size(), 5);
		assertSame(includeNullChunk.size(), 5);
	}
	
	
	@Test
	public void testClone() {
		Chunk c = new Chunk();
		c = chunk.clone();
		for(int i = 0; i < 5; i++) {
			assertEquals(c.get(i), chunk.get(i));
		}
		assertSame(chunk.size(), c.size());
		
		c = includeNullChunk.clone();
		for(int i = 0; i < 5; i++) {
			assertEquals(c.get(i), includeNullChunk.get(i));
		}
	}
	
	
	@Test
	@Ignore
	public void testToChunk() {
		Chunk<Element> c = chunk.toChunk(new LongRange(1, 3));
		assertSame(c.size(), 2);
		for(int i = 1; i < 3; i++) {
			assertEquals(c.get(i), elems[i]);
		}
		
		c = chunk.toChunk(new LongRange(-1, 6));
		assertSame(c.size(), 5);
		for(int i = 0; i < 5; i++) {
			assertEquals(c.get(i), elems[i]);
		}
		
		c = chunk.toChunk(new LongRange(-1, 3));
		assertSame(c.size(), 3);
		for(int i = 0; i < 3; i++) {
			assertEquals(c.get(i), elems[i]);
		}
	}
	
	
	@Test(expected = IndexOutOfBoundsException.class)
	public void testToChunkError() {
		chunk.toChunk(new LongRange(5, 10));
	}
	
	
	@Test
	public void testSubList() {
		RangedList<Element> subList = chunk.subList((long)1, (long)3);
		assertSame(subList.longSize(), (long)2);
		for(int i = 1; i < 3; i++) {
			assertEquals(subList.get(i), elems[i]);
		}
		
		subList = chunk.subList((long)-1, (long)6);
		assertSame(subList.longSize(), (long)5);
		for(int i = 0; i < 5; i++) {
			assertEquals(subList.get(i), elems[i]);
		}
		
		subList = chunk.subList((long)-1, (long)3);
		assertSame(subList.longSize(), (long)3);
		for(int i = 0; i < 3; i++) {
			assertEquals(subList.get(i), elems[i]);
		}
	}
	
	
	@Test(expected = IllegalArgumentException.class)
	public void testSubListIllegalArguments() {
		chunk.subList(10l, 5l);
	}
	
	@Test
	public void testForEach() {
		chunk.forEach(e -> e.increase(5));
		for(int i = 0; i < 5; i++) {
			assertSame(chunk.get(i).n, i + 5);
		}
	}
	
}
