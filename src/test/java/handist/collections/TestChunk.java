package handist.collections;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestChunk implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = -2700365175790886892L;


	public class Element implements Serializable {
		/**Serial Version UID */
		private static final long serialVersionUID = -300902383514143401L;
		public int n = 0;
		public Element(int i) {
			n = i;
		}
		public void increase(int i) {
			n += i;
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

	
	@Before
	public void setUp() throws Exception {
		chunk = new Chunk<>(new LongRange(0, 5));
		includeNullChunk = new Chunk<Element>(new LongRange(0, 5));
		
		for(int i = 0; i < 5; i++) {
			elems[i] = new Element(i);
			chunk.set(i, elems[i]);
		}
		includeNullChunk.set(0, null);
		
	}
	
	@After
	public void tearDown() throws Exception {
	}

	
	@Test(expected = UnsupportedOperationException.class)
	public void testMake() {
		ArrayList<Element> list = new ArrayList<Element>();
		for(int i = 0; i < elems.length; i++) {
			list.add(elems[i]);
		}
		chunk = Chunk.make(list, new LongRange(0, elems.length));
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testMakeWithParameter() {
		ArrayList<Element> list = new ArrayList<Element>();
		for(int i = 0; i < elems.length; i++) {
			list.add(elems[i]);
		}
		chunk = Chunk.make(list, new LongRange(0, elems.length), new Element(0));
	}
	
	
	@Test
	public void testGetRange() {
		LongRange newRange = chunk.getRange();
		assertSame(newRange.from, (long)0);
		assertSame(newRange.to, (long)elems.length);
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
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testClear() {
		chunk.clear();
	}
	
	
	@Test
	public void testClone() {
		Chunk<Element> c = chunk.clone();
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
	public void testCloneRange() {
		//same range
		Chunk<Element> c = chunk.cloneRange(chunk.range);
		for(int i = 0; i < elems.length; i++) {
			assertEquals(c.get(i), chunk.get(i));
		}
		
		//inner range
		c = chunk.cloneRange(new LongRange(0, 3));
		assertSame(c.longSize(), (long)3);
		for(int i = 0; i < 3; i++) {
			assertEquals(c.get(i), chunk.get(i));
		}
	}
	
	
	@Ignore
	@Test
	public void testToChunk() {
		//same size
		Chunk<Element> c = chunk.toChunk(chunk.getRange());
		for(int i = 0; i < elems.length; i++) {
			assertEquals(c.get(i), elems[i]);
		}
		
		//inner size
		c = chunk.toChunk(new LongRange(1, 3));
		assertSame(c.size(), 2);
		for(int i = 1; i < 3; i++) {
			assertEquals(c.get(i), elems[i]);
		}
	
		//both over size
		c = chunk.toChunk(new LongRange(-1, 6));
		assertSame(c.size(), 5);
		for(int i = 0; i < 5; i++) {
			assertEquals(c.get(i), elems[i]);
		}
		
		//oneside over size		// chunk.toArray / error in arrayCopy 
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
		//same range
		RangedList<Element> subList = chunk.subList(chunk.range.from, chunk.range.to);
		assertSame(subList.longSize(), chunk.longSize());
		for(int i = 0; i < subList.longSize(); i++) {
			assertEquals(subList.get(i), elems[i]);
		}
		
		//inner range
		subList = chunk.subList((long)1, (long)3);
		assertSame(subList.longSize(), (long)2);
		for(int i = 1; i < 3; i++) {
			assertEquals(subList.get(i), elems[i]);
		}
		
		//both over range
		subList = chunk.subList((long)-1, (long)6);
		assertSame(subList.longSize(), (long)5);
		for(int i = 0; i < 5; i++) {
			assertEquals(subList.get(i), elems[i]);
		}
		
		//oneside over range
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
	public void testSize() {
		assertSame(chunk.size(), 5);
		assertSame(includeNullChunk.size(), 5);
	}
	
	
	@Test
	public void testLongSize() {
		assertSame(chunk.longSize(), (long)5);
	}
	
	
	@Test(expected = Error.class)
	public void testLongSizeError() {
		chunk.range = null;
		chunk.longSize();
	}
	
	
	@Ignore
	@Test
	public void testToArray() {
		// same range
		Object[] a = chunk.toArray();
		for(int i = 0; i < a.length; i++) {
			assertEquals(a[i], elems[i]);
		}
		
		a = chunk.toArray(chunk.getRange());
		for(int i = 0; i < elems.length; i++) {
			assertEquals(a[i], elems[i]);
		}
		
		//inner range
		a = chunk.toArray(new LongRange(1, 3));
		for(int i = 1; i < 3; i++) {
			assertEquals(a[i-1], elems[i]);
		}
		
		//both over range
		a = chunk.toArray(new LongRange(-1, 6));
		for(int i = 0; i < elems.length; i++) {
			assertEquals(a[i], elems[i]);
		}
		
		//oneside over range
		a = chunk.toArray(new LongRange(-1, 3));
		for(int i = 0; i < 3; i++) {
			assertEquals(a[i], elems[i]);
		}
		
	}
	
	
	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testToArrayWithParameters() {
		chunk.toArray(new LongRange(-5, -4));
	}
	
	
	@Test(expected = IllegalArgumentException.class)
	public void testToArrayHugeSize() {
		chunk.toArray(new LongRange(1, Config.maxChunkSize + 10));
	}
	

	@Test(expected = IllegalArgumentException.class)
	public void testHugeChunk() {
		new Chunk<>(new LongRange(0, Config.maxChunkSize + 10));
	}
	
	
	@Test(expected = IllegalArgumentException.class)
	public void testHugeChunkWithValue() {
		new Chunk<>(new LongRange(0, Config.maxChunkSize + 10), (byte)0);
	}
	
	@Test(expected = NullPointerException.class)
	public void testChunkNullRange() {
		new Chunk<>(null, elems);
	}
	
	
	@Test
	public void testMap() {
		RangedList<Integer> c = chunk.map(e -> e.n + 5);
		for(int i = 0; i < elems.length; i++) {
			assertSame(c.get(i), elems[i].n + 5);
		}
	}
	
	
	@Test(expected = RuntimeException.class)
	public void testSetupFromError() {
		// test setupFrom throw Exception
		chunk.range = new LongRange(0, (long)Integer.MAX_VALUE + 10);
		chunk.setupFrom(chunk, e -> new Element(e.n + 2));
	}
	
	
	// -- test iterator functions region --
	@Test
	public void testIteratorHasNext() {
		Iterator<Element> it = chunk.iterator();
		for(int i = -1; i < elems.length - 1; i++) {
			assertTrue(it.hasNext());
			it.next();
		}
		assertFalse(it.hasNext());
		
		it = chunk.iteratorFrom(3);
		for(int i = 2; i < elems.length - 1; i++) {
			assertTrue(it.hasNext());
			it.next();
		}
		assertFalse(it.hasNext());
	}
	
	
	@Test
	public void testIteratorNext() {
		Iterator<Element> it = chunk.iterator();
		for(int i = 0; i < elems.length; i++) {
			assertEquals(it.next(), elems[i]);
		}
	}
	
	
	@Test
	public void testIteratorHasPrevious() {
		ListIterator<Element> it = chunk.listIterator();
		assertFalse(it.hasPrevious());
		it = chunk.listIterator(2);
		assertTrue(it.hasPrevious());
		assertEquals(it.previous(), elems[0]);
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testIteratorNextIndex() {
		ListIterator<Element> it = chunk.listIterator();
		it.nextIndex();
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testIteratorPreviousIndex() {
		ListIterator<Element> it = chunk.listIterator();
		it.previousIndex();
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testIteratorRemove() {
		ListIterator<Element> it = chunk.listIterator();
		it.remove();
	}
	
	
	@Test
	public void testIteratorSet() {
		ListIterator<Element> it = chunk.listIterator(1);
		Element e = new Element(-1);
		it.set(e);
		assertEquals(chunk.get(0), e);
	}
	

	@Test(expected = UnsupportedOperationException.class)
	public void testIteratorAdd() {
		ListIterator<Element> it = chunk.listIterator();
		it.add(new Element(0));
	}
	
	
	@Test(expected = ArrayIndexOutOfBoundsException.class)
	public void testIteratorFromError() {
		chunk.iteratorFrom(100);
	}
	
	// -- end iterator region --
	
	
	@Test
	public void testForEach() {
		chunk.forEach(e -> e.increase(5));
		for(int i = 0; i < 5; i++) {
			assertSame(chunk.get(i).n, i + 5);
		}
		
		//TODO :test forEach that has parameter BiConsumer 
	}
	
	
	@Test
	public void testToString() {
		assertEquals("[[0,5)]:0,1,2,3,4", chunk.toString());
		
		chunk.range = null;
		assertEquals("[Chunk] in Construction", chunk.toString());
	
		// test omitElementsToString 
		chunk = new Chunk<>(new LongRange(10, 100));
		for(int i = 10; i < 100; i++) {
			chunk.set(i, new Element(i));
		}
		assertEquals("[[10,100)]:10,11,12,13,14,15,16,17,18,19...(omitted 80 elements)", chunk.toString());
	}
	
	
	// use ObjectOutputStream
	// test writeObject and readObject
	@Test
	public void testWriteObject() throws IOException, ClassNotFoundException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
		objectOut.writeObject(chunk);
		
		byte[] buf = byteOut.toByteArray();
		
		ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
		ObjectInputStream objectIn = new ObjectInputStream(byteIn);
		@SuppressWarnings("unchecked")
		Chunk<Element> readChunk = (Chunk<Element>)objectIn.readObject();
		
		for(int i = 0; i < elems.length; i++) {
			assertSame(readChunk.get(i).n, chunk.get(i).n);
		}
		
		byteOut.close();
		objectOut.close();
		byteIn.close();
		objectIn.close();
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testAddAll() {
		chunk.addAll(1, new ArrayList<Element>());
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testAdd() {
		chunk.add(1, new Element(-1));
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
		chunk.remove(0);
	}

	
	@Test(expected = UnsupportedOperationException.class)
	public void testIndexOf() {
		chunk.indexOf(elems[1]);
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testLastIndexOf() {
		chunk.lastIndexOf(elems[1]);
	}
	
	
	@Test(expected = UnsupportedOperationException.class)
	public void testSubListInt() {
		chunk.subList(1, 3);
	}
}
