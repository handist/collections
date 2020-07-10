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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Junit test class for class {@link RangedListView}. 
 * @author Patrick
 *
 */
public class TestRangedListView {

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

	private static final int ELEMENTS_COUNT = 10;
	private Chunk<Element> chunk;
	private Element[] elems;
	private LongRange range;
	private RangedListView<Element> view;

	@Before
	public void setUp() throws Exception {
		elems = new Element[ELEMENTS_COUNT];
		chunk = new Chunk<>(new LongRange(0, ELEMENTS_COUNT));
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			elems[i] = new Element(i);
			chunk.set(i, elems[i]);
		}

		chunk.set(2, null);	//include null test

		range = new LongRange(1, 5);
		view = new RangedListView<>(chunk, range);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test(expected = java.lang.UnsupportedOperationException.class)
	public void testClear() {
		view.clear();
	}

	@Test
	public void testClone() {
		RangedList<Element> v = view.clone();
		assertSame(v.longSize(), view.longSize());
	}

	@Test
	public void testContains() {
		assertTrue(view.contains(elems[1]));
		assertFalse(view.contains(elems[9]));
		assertFalse(view.contains(new Element(100)));		
	}

	@Test
	@Ignore
	public void testForEach() {
		view.forEach(e -> System.out.println("forEach :" + e));
	}

	/**
	 * Checks that the elements inserted during the {@link #setUp()} 
	 * method are correctly inserted in the view.
	 */
	@Test
	public void testGet() {
		/*for (int i = 0; i < ELEMENTS_COUNT; i++)*/
		view.getRange().forEach((long i)-> {
			if (i != 2) {
				assertEquals(elems[(int)i], view.get(i));				
			} else {
				assertEquals(null, view.get(i));
			}
		});
	}

	@Test
	public void testGetExceptionLong() {
		long[] indices = { -1, 1L << 33 + 1, -1L << 34 + 1 };
		for (long index : indices) {
			assertThrows(IndexOutOfBoundsException.class, () -> {
				chunk.get(index);
			});
		}
	}

	@Test
	public void testLongSize() {
		assertEquals(4l, view.longSize());
		RangedListView<Element> emptyView = RangedListView.emptyView();
		assertEquals(0l, emptyView.longSize());
	}

	/**
	 * Checks that the element set into the view during the {@link #setUp()}
	 * method is correctly replaced by calling {@link RangedListView#set(long, Object)}.
	 */
	@Test
	public void testSet() {
		assertEquals(null, view.get(2));
		Element e = new Element(42);
		view.set(2, e);
		assertEquals(e, view.get(2));
	}

	@Test
	public void testSetExceptionLong() {
		long[] indices = { -1, 1L << 33 + 1, -1L << 34 + 1 };
		for (long index : indices) {
			assertThrows(IndexOutOfBoundsException.class, () -> {
				chunk.set(index, elems[0]);
			});
		}
	}

	@Test
	public void testSetupFrom() {
		//yet
	}

	@Test
	public void testSize() {
		assertEquals(4, view.size());

		RangedListView<Element> emptyView = RangedListView.emptyView();
		assertEquals(0, emptyView.size());
	}


	@Test
	public void testSubList() {
		RangedList<Element> sub = view.subList(new LongRange(2, 4));
		assertSame(sub.longSize(), (long)2);
		assertEquals(sub.get(2), view.get(2));
	}


	@Test
	public void testToArray() {
		Object[] o = view.toArray();
		assertSame(o.length, 4);
		assertEquals(o[0], elems[1]);
		assertNull(o[1]);
	}


	@Test
	public void testToChunk() {
		Chunk<Element> chunk2to4 = view.toChunk(new LongRange(2, 4));
		assertEquals(2, chunk2to4.size());
		assertEquals(chunk2to4.get(3), elems[3]);

		Chunk<Element> chunk5to9 = view.toChunk(new LongRange(5, 9));
		assertEquals(4, chunk5to9.size());
		for (int i = 5; i < 9; i ++) {
			assertEquals(elems[i], chunk5to9.get(i));
		}
		try {
			chunk5to9.get(9);
			fail("Accessing an index out of range should have thrown an error");
		} catch (IndexOutOfBoundsException e) {
		}
	}

	/**
	 * Checks that a IndexOutOfBoundsException is thrown
	 * when a bad range is given as parameter.
	 */
	@Test(expected = IndexOutOfBoundsException.class)
	public void testToChunkBadRange() {
		view.toChunk(new LongRange(-1, 6));
	}
}
