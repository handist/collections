/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

public class TestChunk implements Serializable {

    /** Element Class for Chunk */
    public class Element implements Serializable {
        /** Serial Version UID */
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
    /** chunk include null member in index 0 */
    private Chunk<Element> includeNullChunk;

    /**
     * elems[i] = new Element(i); chunk : { elems[0], elems[1], ... elems[4] }
     * includeNullChunk : { null , elems[1], ... elems[4] }
     */
    @Before
    public void setUp() {
        chunk = new Chunk<>(new LongRange(0, 5));
        includeNullChunk = new Chunk<>(new LongRange(0, 5));

        for (int i = 0; i < 5; i++) {
            elems[i] = new Element(i);
            chunk.set(i, elems[i]);
        }
        includeNullChunk.set(0, null);
    }

//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testAdd() {
//		chunk.add(1, new Element(-1));
//	}

//
//	// Unsupported Functions
//	@Test(expected = UnsupportedOperationException.class)
//	public void testAddAll() {
//		chunk.addAll(1, new ArrayList<Element>());
//	}
//
//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testClear() {
//		chunk.clear();
//	}

    @Test
    public void testClone() {
        Chunk<Element> c = chunk.clone();
        for (int i = 0; i < 5; i++) {
            assertEquals(c.get(i), chunk.get(i));
        }
        assertSame(chunk.size(), c.size());

        c = includeNullChunk.clone();
        for (int i = 0; i < 5; i++) {
            assertEquals(c.get(i), includeNullChunk.get(i));
        }
    }

    @Test
    public void testCloneRange() {
        // same range
        Chunk<Element> c = chunk.cloneRange(chunk.getRange());
        for (int i = 0; i < elems.length; i++) {
            assertEquals(c.get(i), chunk.get(i));
        }

        // inner range
        c = chunk.cloneRange(new LongRange(0, 3));
        assertSame(c.size(), (long) 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(c.get(i), chunk.get(i));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorDiffRange() {
        final Object[] o = new Object[100];
        new Chunk<>(new LongRange(0, 1), o);
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
    public void testConstructorWithInitializer() {
        final Function<Long, Element> init = (Long index) -> {
            return new Element(107 * index.intValue());
        };
        final LongRange range = new LongRange(10, 100);
        final Chunk<Element> c = new Chunk<>(range, init);
        c.forEach((long i, Element e) -> {
            assertEquals(init.apply(i).n, e.n);
        });
    }

    @Test
    public void testConstructorWithParameter() {
        Element e = new Element(1);
        Chunk<Element> c = new Chunk<>(new LongRange(0, 5), e);
        for (int i = 0; i < 5; i++) {
            assertEquals(c.get(i), e);
        }

        e = null;
        c = new Chunk<>(new LongRange(1, 5), e);
        assertSame(c.size(), 4l);
        for (int i = 1; i < 5; i++) {
            assertNull(c.get(i));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorZeroSize() {
        new Chunk<>(new LongRange(0, 0));
    }

    @Test
    public void testContains() {
        for (int i = 0; i < 5; i++) {
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

        for (int i = 0; i < chunk.size(); i++) {
            assertSame(chunk.get(i).n, i + 5);
        }
    }

    @Test
    public void testForEachWithIndex() {
        // test forEach that has parameter LTConsumer
        chunk.forEach((l, e) -> {
            e.n += l;
        });

        for (int i = 0; i < chunk.size(); i++) {
            assertSame(chunk.get(i).n, i + i);
        }
    }

    @Test
    public void testForEachWithReceiver() {
        // test forEach that has parameter BiConsumer
        chunk.forEach(new LongRange(0, 5), (e, reciever) -> {
            reciever.accept(e);
        }, (e -> ((Element) e).increase(5)));

        for (int i = 0; i < chunk.size(); i++) {
            assertSame(chunk.get(i).n, i + 5);
        }
    }

    @Test
    public void testGet() {
        for (int i = 0; i < 5; i++) {
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
        final long[] indices = { -1, 1L << 33 + 1, -1L << 34 + 1 };
        for (final long index : indices) {
            assertThrows(IndexOutOfBoundsException.class, () -> {
                chunk.get(index);
            });
        }
    }

    @Test
    public void testGetRange() {
        final LongRange newRange = chunk.getRange();
        assertSame(newRange.from, (long) 0);
        assertSame(newRange.to, (long) elems.length);
    }

//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testIndexOf() {
//		chunk.indexOf(elems[1]);
//	}

//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testIteratorAdd() {
//		ListIterator<Element> it = chunk.listIterator();
//		it.add(new Element(0));
//	}

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIteratorErrorIndex() {
        chunk.iterator(100l);
    }

    /*
     * @Test(expected = Error.class) public void testLongSizeError() { chunk.range =
     * null; chunk.longSize(); }
     */

    @Test
    public void testIteratorHasNext() {
        Iterator<Element> it = chunk.iterator();
        for (int i = -1; i < elems.length - 1; i++) {
            assertTrue(it.hasNext());
            it.next();
        }
        assertFalse(it.hasNext());

        it = chunk.iterator(3l);
        for (int i = 2; i < elems.length - 1; i++) {
            assertTrue(it.hasNext());
            it.next();
        }
        assertFalse(it.hasNext());
    }

    @Test
    public void testIteratorHasPrevious() {
        RangedListIterator<Element> it = chunk.iterator();
        assertFalse(it.hasPrevious());
        it = chunk.iterator(2l);
        assertTrue(it.hasPrevious());
        assertEquals(it.previous(), elems[1]);
    }

    @Test(expected = IllegalStateException.class)
    public void testIteratorIllegalState() {
        chunk.iterator().set(null);
    }

    @Test
    public void testIteratorNext() {
        final Iterator<Element> it = chunk.iterator();
        for (int i = 0; i < elems.length; i++) {
            assertEquals(it.next(), elems[i]);
        }
    }

    @Test
    public void testIteratorNextIndex() {
        final RangedListIterator<Element> it = chunk.iterator();
        assertEquals(0l, it.nextIndex());
    }

//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testIteratorRemove() {
//		ListIterator<Element> it = chunk.listIterator();
//		it.remove();
//	}

    @Test
    public void testIteratorPreviousIndex() {
        final RangedListIterator<Element> it = chunk.iterator();
        assertEquals(-1l, it.previousIndex());
    }

    @Test
    public void testIteratorSet() {
        // FIXME this is not good, we need to check the IllegalStateException
        // Check the fact the "last object returned" by either previous or next
        // is modified
        final Element e = new Element(-1);
        final RangedListIterator<Element> it = chunk.iterator(1l);
        final Element oldValue = it.next();
        assertEquals(elems[1], oldValue);
        it.set(e); // Replace oldValue with e
        assertEquals(e, chunk.get(1l));

        // Call previous to check the same element is given back (in between
        // values implementation of iterator)
        assertEquals(e, it.previous());
        it.set(oldValue);
        assertEquals(oldValue, chunk.get(1l));
    }

//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testLastIndexOf() {
//		chunk.lastIndexOf(elems[1]);
//	}

    @Test
    public void testMap() {
        final RangedList<Integer> c = chunk.map(e -> e.n + 5);
        for (int i = 0; i < elems.length; i++) {
            assertSame(c.get(i), elems[i].n + 5);
        }
    }

    @Test
    public void testSet() {
        final Element newElement = new Element(100);
        assertEquals(chunk.get(0), elems[0]);
        chunk.set(0, newElement);
        assertEquals(chunk.get(0), newElement);
    }

    /*
     * @Test(expected = IndexOutOfBoundsException.class) public void
     * testSetupFromError() { // test setupFrom throw Exception chunk.range = new
     * LongRange(0, (long)Integer.MAX_VALUE + 10); chunk.setupFrom(chunk, e -> new
     * Element(e.n + 2)); }
     */
//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testRemove() {
//		chunk.remove(0);
//	}

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetError() {
        chunk.set(6, new Element(0));
    }

    @Test
    public void testSetExceptionLong() {
        final long[] indices = { -1, 1L << 33 + 1, -1L << 34 + 1 };
        for (final long index : indices) {
            assertThrows(IndexOutOfBoundsException.class, () -> {
                chunk.set(index, elems[0]);
            });
        }
    }

    @Test
    public void testSize() {
        assertSame(chunk.size(), 5l);
        assertSame(includeNullChunk.size(), 5l);
    }

    @Test
    public void testSubList() {
        // same range
        RangedList<Element> subList = chunk.subList(chunk.getRange().from, chunk.getRange().to);
        assertSame(subList.size(), chunk.size());
        for (int i = 0; i < subList.size(); i++) {
            assertEquals(subList.get(i), elems[i]);
        }

        // inner range
        subList = chunk.subList(1l, 3l);
        assertSame(subList.size(), 2l);
        for (int i = 1; i < 3; i++) {
            assertEquals(subList.get(i), elems[i]);
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void testSubListIllegalRange() {
        chunk.subList(5, 4);
    }

//
//	@Test(expected = UnsupportedOperationException.class)
//	public void testSubListInt() {
//		chunk.subList(1, 3);
//	}

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSubListOutRange() {
        chunk.subList(10l, 12l);
    }

    @Test
    public void testSubListOverRange() {
        assertEquals(chunk.subList(-1, 1).getRange(), new LongRange(0, 1));
    }

    @Test
    public void testToArray() {
        Object[] a = chunk.toArray();
        for (int i = 0; i < a.length; i++) {
            assertEquals(a[i], elems[i]);
        }

        // same range
        a = chunk.toArray(chunk.getRange());
        for (int i = 0; i < elems.length; i++) {
            assertEquals(a[i], elems[i]);
        }

        // inner range
        a = chunk.toArray(new LongRange(1, 3));
        for (int i = 1; i < 3; i++) {
            assertEquals(a[i - 1], elems[i]);
        }

        // zero range
        a = chunk.toArray(new LongRange(1, 1));
        assertSame(a.length, 0);
    }

    @SuppressWarnings("rawtypes")
    @Test(expected = IllegalArgumentException.class)
    public void testToArrayHugeSize() {
        new Chunk(new LongRange(10L, 10L + Config.maxChunkSize + 100)).toArray(new LongRange(20L, Config.maxChunkSize));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testToArrayOutRange() {
        chunk.toArray(new LongRange(-5, -4));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testToArrayOverRange() {
        final Object[] a = chunk.toArray(new LongRange(-1, 4));
        for (int i = 0; i < 4; i++) {
            assertEquals(a[i], elems[i]);
        }
    }

    @Test
    public void testToChunk() {
        // same size
        Chunk<Element> c = chunk.toChunk(chunk.getRange());
        for (int i = 0; i < elems.length; i++) {
            assertEquals(c.get(i), elems[i]);
        }

        // inner size
        c = chunk.toChunk(new LongRange(1, 3));
        assertSame(c.size(), 2l);
        for (int i = 1; i < 3; i++) {
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
         * chunk.range = null; assertEquals(chunk.toString(),
         * "[Chunk] in Construction");
         */
        // test omitElementsToString
        chunk = new Chunk<>(new LongRange(10, 100));
        for (int i = 10; i < 100; i++) {
            chunk.set(i, new Element(i));
        }
        assertEquals(chunk.toString(), "[[10,100)]:10,11,12,13,14,15,16,17,18,19...(omitted 80 elements)");
    }

    // use ObjectOutputStream
    // test writeObject and readObject
    @Test
    public void testWriteObject() throws IOException, ClassNotFoundException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
        objectOut.writeObject(chunk);
        objectOut.close();

        final byte[] buf = byteOut.toByteArray();

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
        final ObjectInputStream objectIn = new ObjectInputStream(byteIn);
        @SuppressWarnings("unchecked")
        final Chunk<Element> readChunk = (Chunk<Element>) objectIn.readObject();

        for (int i = 0; i < elems.length; i++) {
            assertSame(readChunk.get(i).n, chunk.get(i).n);
        }

        objectIn.close();
    }

    @Test
    public void testWriteObjectKryo() {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final ObjectOutput objectOut = new ObjectOutput(byteOut);
        objectOut.writeObject(chunk);
        objectOut.close();

        final byte[] buf = byteOut.toByteArray();

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
        final ObjectInput objectIn = new ObjectInput(byteIn);
        @SuppressWarnings("unchecked")
        final Chunk<Element> readChunk = (Chunk<Element>) objectIn.readObject();

        for (int i = 0; i < elems.length; i++) {
            assertSame(readChunk.get(i).n, chunk.get(i).n);
        }
        objectIn.close();
    }
}
