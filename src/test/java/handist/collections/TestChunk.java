/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
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
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

public class TestChunk implements Serializable {

	/** Element Class for Chunk */
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


	/** Serial Version UID */
	private static final long serialVersionUID = -2700365175790886892L;

	/** chunk filled with 5 initial members */
	private Chunk<Element> chunk;
	/** Contains 5 initialized instances of class Element */
	Element[] elems = new Element[5];
	/** chunk include null member in index 0*/
	private Chunk<Element> includeNullChunk;


	/** 
	 * elems[i] = new Element(i);
	 * chunk 			: { elems[0], elems[1], ... elems[4] }
	 * includeNullChunk : {  null   , elems[1], ... elems[4] }
	 */
	@Before
	public void setUp() {
		chunk = new Chunk<>(new LongRange(0, 5));
		includeNullChunk = new Chunk<Element>(new LongRange(0, 5));

		for(int i = 0; i < 5; i++) {
			elems[i] = new Element(i);
			chunk.set(i, elems[i]);
		}
		includeNullChunk.set(0, null);
	}


	@Test(expected = UnsupportedOperationException.class)
	public void testAdd() {
		chunk.add(1, new Element(-1));
	}


	// Unsupported Functions
	@Test(expected = UnsupportedOperationException.class)
	public void testAddAll() {
		chunk.addAll(1, new ArrayList<Element>());
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
		Chunk<Element> c = chunk.cloneRange(chunk.getRange());
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


	@Test(expected = IllegalArgumentException.class)
	public void testConstructorDiffRange() {
		Object[] o = new Object[100];
		new Chunk<Object>(new LongRange(0, 1), o);
	}


	@Test(expected = IllegalArgumentException.class)
	public void testConstructorHugeSize() {
		new Chunk<>(new LongRange(0, Config.maxChunkSize + 10));
	}


	@Test(expected = NullPointerException.class)
	public void testConstructorNullRange() {
		new Chunk<>(null, elems);
	}

	@Test
	public void testConstructorWithParameter() {
        Element e = new Element(1);
        Chunk<Element> c = new Chunk<>(new LongRange(0, 5), e);
        for(int i = 0; i < 5; i++) {
            assertEquals(c.get(i), e);
        }

        e = null;
        c = new Chunk<>(new LongRange(1, 5), e);
        assertSame(c.size(), 4);
        for(int i = 1; i < 5; i++) {
            assertNull(c.get(i));
        }
	}
	
   @Test
   public void testConstructorWithInitializer() {
       Function<Long,Element> init= (Long index)->{
         return new Element(107 * index.intValue());
       };
       LongRange range = new LongRange(10, 100);
       Chunk<Element> c = new Chunk<>(range, init);
       c.forEach((long i, Element e) -> {
           assertEquals(init.apply(i).n, e.n);
       });
   }

	@Test(expected = IllegalArgumentException.class)
	public void testConstructorZeroSize() {
		new Chunk<>(new LongRange(0, 0));
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


	// normal forEach
	@Test
	public void testForEach() {
		chunk.forEach(e -> e.increase(5));

		for(int i = 0; i < chunk.size(); i++) {
			assertSame(chunk.get(i).n, i + 5);
		}
	}


	@Test
	public void testForEachWithIndex() {
		// test forEach that has parameter LTConsumer
		chunk.forEach((l, e) -> {e.n += l;} );

		for(int i = 0; i < chunk.size(); i++) {
			assertSame(chunk.get(i).n, i + i);
		}
	}


	@Test
	public void testForEachWithReceiver() {
		// test forEach that has parameter BiConsumer
		chunk.forEach(new LongRange(0, 5), (e, reciever) -> {
			reciever.accept(e);
		}, (e -> ((Element) e).increase(5)));

		for(int i = 0; i < chunk.size(); i++) {
			assertSame(chunk.get(i).n, i + 5);
		}
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
	public void testGetExceptionLong() {
		long[] indices = { -1, 1L<<33 + 1,  -1L<< 34 + 1 };
		for(long index: indices) {
			assertThrows(IndexOutOfBoundsException.class, ()->{
				chunk.get(index); 
			});
		}
	}

	@Test
	public void testGetRange() {
		LongRange newRange = chunk.getRange();
		assertSame(newRange.from, (long)0);
		assertSame(newRange.to, (long)elems.length);
	}


	@Test(expected = UnsupportedOperationException.class)
	public void testIndexOf() {
		chunk.indexOf(elems[1]);
	}


	@Test(expected = UnsupportedOperationException.class)
	public void testIteratorAdd() {
		ListIterator<Element> it = chunk.listIterator();
		it.add(new Element(0));
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testIteratorFromError() {
		chunk.iteratorFrom(100);
	}

	/*
	@Test(expected = Error.class)
	public void testLongSizeError() {
		chunk.range = null;
		chunk.longSize();
	}
	 */

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
	public void testIteratorHasPrevious() {
		ListIterator<Element> it = chunk.listIterator();
		assertFalse(it.hasPrevious());
		it = chunk.listIterator(2);
		assertTrue(it.hasPrevious());
		assertEquals(it.previous(), elems[0]);
	}


	@Test
	public void testIteratorNext() {
		Iterator<Element> it = chunk.iterator();
		for(int i = 0; i < elems.length; i++) {
			assertEquals(it.next(), elems[i]);
		}
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
	public void testLastIndexOf() {
		chunk.lastIndexOf(elems[1]);
	}


	@Test
	public void testLongSize() {
		assertSame(chunk.longSize(), (long)5);
	}


	@Test
	public void testMap() {
		RangedList<Integer> c = chunk.map(e -> e.n + 5);
		for(int i = 0; i < elems.length; i++) {
			assertSame(c.get(i), elems[i].n + 5);
		}
	}

	/*
	@Test(expected = IndexOutOfBoundsException.class)
	public void testSetupFromError() {
		// test setupFrom throw Exception
		chunk.range = new LongRange(0, (long)Integer.MAX_VALUE + 10);
		chunk.setupFrom(chunk, e -> new Element(e.n + 2));
	}
	 */

	@Test(expected = UnsupportedOperationException.class)
	public void testRemove() {
		chunk.remove(0);
	}


	@Test
	public void testSet() {
		Element newElement = new Element(100);
		assertEquals(chunk.get(0), elems[0]);
		chunk.set(0, newElement);
		assertEquals(chunk.get(0), newElement);
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testSetError() {
		chunk.set(6, new Element(0));
	}


	@Test
	public void testSetExceptionLong() {
		long[] indices = { -1, 1L<<33 + 1,  -1L<< 34 + 1 };
		for(long index: indices) {
			assertThrows(IndexOutOfBoundsException.class, ()->{
				chunk.set(index, elems[0]); 
			});
		}
	}


	@Test
	public void testSize() {
		assertSame(chunk.size(), 5);
		assertSame(includeNullChunk.size(), 5);
	}


	@Test
	public void testSubList() {
		//same range
		RangedList<Element> subList = chunk.subList(chunk.getRange().from, chunk.getRange().to);
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

	}


	@Test(expected = IllegalArgumentException.class)
	public void testSubListIllegalRange() {
		chunk.subList((long)5, (long)4);
	}


	@Test(expected = UnsupportedOperationException.class)
	public void testSubListInt() {
		chunk.subList(1, 3);
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testSubListOutRange() {
		chunk.subList((long)10, (long)12);
	}

	// -- end iterator region --


	@Test
	public void testSubListOverRange() {
		assertEquals(chunk.subList((long)-1, (long)1).getRange(), new LongRange(0, 1));
	}


	@Test
	public void testToArray() {
		Object[] a = chunk.toArray();
		for(int i = 0; i < a.length; i++) {
			assertEquals(a[i], elems[i]);
		}

		// same range
		a = chunk.toArray(chunk.getRange());
		for(int i = 0; i < elems.length; i++) {
			assertEquals(a[i], elems[i]);
		}

		// inner range
		a = chunk.toArray(new LongRange(1, 3));
		for(int i = 1; i < 3; i++) {
			assertEquals(a[i-1], elems[i]);
		}

		// zero range
		a = chunk.toArray(new LongRange(1, 1));
		assertSame(a.length, 0);
	}


	@Test(expected = IllegalArgumentException.class)
	public void testToArrayHugeSize() {
		new Chunk(new LongRange(10L, 10L+Config.maxChunkSize+100)).toArray(new LongRange(20L, Config.maxChunkSize));
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testToArrayOutRange() {
		chunk.toArray(new LongRange(-5, -4));
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testToArrayOverRange() {
		Object[] a = chunk.toArray(new LongRange(-1, 4));
		for(int i = 0; i < 4; i++) {
			assertEquals(a[i], elems[i]);
		}
	}


	@Test
	public void testToChunk() {
		// same size
		Chunk<Element> c = chunk.toChunk(chunk.getRange());
		for(int i = 0; i < elems.length; i++) {
			assertEquals(c.get(i), elems[i]);
		}

		// inner size
		c = chunk.toChunk(new LongRange(1, 3));
		assertSame(c.size(), 2);
		for(int i = 1; i < 3; i++) {
			assertEquals(c.get(i), elems[i]);
		}
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testToChunkOutRange() {
		chunk.toChunk(new LongRange(5, 10));
	}


	@Test(expected = IndexOutOfBoundsException.class)
	public void testToChunkOverRange() {
		chunk.toChunk(new LongRange(1, 6));
	}


	@Test(expected = IllegalArgumentException.class)
	public void testToChunkZeroRange() {
		chunk.toChunk(new LongRange(1, 1));
	}


	@Test
	public void testToString() {
		assertEquals(chunk.toString(), "[[0,5)]:0,1,2,3,4");
		/*
		chunk.range = null;
		assertEquals(chunk.toString(), "[Chunk] in Construction");
		 */
		// test omitElementsToString 
		chunk = new Chunk<>(new LongRange(10, 100));
		for(int i = 10; i < 100; i++) {
			chunk.set(i, new Element(i));
		}
		assertEquals(chunk.toString(), "[[10,100)]:10,11,12,13,14,15,16,17,18,19...(omitted 80 elements)");
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
}
