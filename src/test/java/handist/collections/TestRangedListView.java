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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;

/**
 * Junit test class for class {@link RangedListView}.
 *
 * @author Patrick
 *
 */
public class TestRangedListView implements Serializable {

    public class Element implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = 3711173000651896802L;
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

    /** total number of elements in the Chunk */
    private static final int ELEMENTS_COUNT = 10;

    /** Serial Version UID */
    private static final long serialVersionUID = -1573705810224035903L;
    /** Chunl on which the RangedListView is created */
    private Chunk<Element> chunk;
    /** Elements contained in the Chunk */
    private Element[] elems;
    /** instance of RangedListView under test */
    private RangedListView<Element> view;
    /** Range on which the View is created */
    private LongRange viewRange;

    @Before
    public void setUp() throws Exception {
        elems = new Element[ELEMENTS_COUNT];
        chunk = new Chunk<>(new LongRange(0, ELEMENTS_COUNT));
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            elems[i] = new Element(i);
            chunk.set(i, elems[i]);
        }

        viewRange = new LongRange(1, 5);
        view = new RangedListView<>(chunk, viewRange);
    }

    @After
    public void tearDown() throws Exception {
    }

//	@Test(expected = java.lang.UnsupportedOperationException.class)
//	public void testClear() {
//		view.clear();
//	}

    @Test
    public void testClone() {
        final RangedList<Element> v = view.clone();
        assertSame(v.size(), view.size());
        for (long l = 1; l < 5; l++) {
            assertTrue(chunk.get(l) == v.get(l));
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testConstructorIndexOutOfBoundsException() {
        new RangedListView<>(view, new LongRange(0, 5));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testConstructorNotSupportedClass() {
        class NotASupportedClass<T> extends RangedList<T> {

            @Override
            public RangedList<T> cloneRange(LongRange range) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public boolean contains(Object o) {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public T get(long index) {
                // TODO Auto-generated method stub
                return null;
            }

            //@Override
            protected Object[] getBody() {
                return new Object[0];
            }

            //@Override
            protected long getBodyOffset() {
                return 0;
            }

            @Override
            public LongRange getRange() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Iterator<T> iterator() {
                return null;
            }

            @Override
            public RangedListIterator<T> listIterator() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public RangedListIterator<T> listIterator(long from) {
                return null;
            }

            @Override
            protected Iterator<T> subIterator(LongRange range) {
                return null;
            }

            @Override
            public RangedListIterator<T> subListIterator(LongRange range) {
                return null;
            }

            @Override
            protected RangedListIterator<T> subListIterator(LongRange range, long from) {
                return null;
            }

            @Override
            public T set(long index, T value) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public <S> void setupFrom(RangedList<S> source, Function<? super S, ? extends T> func) {
                // TODO Auto-generated method stub

            }

            @Override
            public Object[] toArray() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Object[] toArray(LongRange newRange) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public Chunk<T> toChunk(LongRange newRange) {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public List<T> toList(LongRange r) {
                // TODO Auto-generated method stub
                return null;
            }

        }
        final NotASupportedClass<Integer> instance = new NotASupportedClass<>();

        new RangedListView<>(instance, new LongRange(0, 0));
    }

    @Test
    public void testContains() {
        assertTrue(view.contains(elems[1]));
        assertFalse(view.contains(elems[9]));
        assertFalse(view.contains(new Element(100)));
    }

    @Test
    public void testEquals() {
        RangedListView<Element> v = new RangedListView<>(chunk, new LongRange(1, 5));
        assertTrue(v.equals(view));
        v = new RangedListView<>(chunk, new LongRange(0, 5));
        assertFalse(v.equals(view));
    }

    @Test
    public void testForEach() {
        // Do something on the view part of the chunk
        view.forEach(e -> e.n *= e.n);

        // Check that only the view part was affected
        for (long l = 0; l < chunk.getRange().to; l++) {
            final long value = chunk.get(l).n;
            if (view.getRange().contains(l)) {
                assertEquals(l * l, value);
            } else {
                assertEquals(l, value);
            }
        }
    }

    @Test
    public void testForEachLongRangeBiConsumer() {
        view.forEach((l, e) -> e.n *= l);

        // Check that only the view part was affected
        for (long l = 0; l < chunk.getRange().to; l++) {
            final long value = chunk.get(l).n;
            if (view.getRange().contains(l)) {
                assertEquals(l * l, value);
            } else {
                assertEquals(l, value);
            }
        }
    }

    @Test
    public void testForEachRangeBiConsumerConsumer() {
        final Consumer<Integer> consumer = new Consumer<Integer>() {
            public Integer sum = new Integer(0);

            @Override
            public void accept(Integer i) {
                sum += i;
            }

            @Override
            public String toString() {
                return sum.toString();
            }
        };
        view.forEach(new LongRange(2, 5), (t, consumerOfU) -> consumerOfU.accept(new Integer(t.n)), consumer);

        final int expected = 2 + 3 + 4;
        assertEquals(Integer.toString(expected), consumer.toString());
    }

    /**
     * Checks that the elements inserted during the {@link #setUp()} method are
     * correctly inserted in the view.
     */
    @Test
    public void testGet() {
        /* for (int i = 0; i < ELEMENTS_COUNT; i++) */
        view.getRange().forEach((long i) -> {
            assertEquals(elems[(int) i], view.get(i));
        });
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
    public void testHashCode() {
        final int firstHash = view.hashCode();
        // Do a little stuff
        for (int i = 0; i < 43; i++) {
            ;
        }
        final int secondHash = view.hashCode();
        // Check that the hash is the same
        assertEquals(firstHash, secondHash);
    }

    @Test
    public void testIsEmpty() {
        assertTrue(RangedListView.emptyView().isEmpty());
        assertFalse(view.isEmpty());
        assertTrue(new RangedListView<>(view, new LongRange(1)).isEmpty());
    }

    @Test
    public void testIterator() {
        final Iterator<Element> it = view.iterator();

        // Do something on the elements of it
        while (it.hasNext()) {
            final Element e = it.next();
            e.n *= e.n;
        }

        assertThrows(IndexOutOfBoundsException.class, () -> it.next());

        // Check only the view part of the chunk was affected
        for (long l = 0; l < chunk.getRange().to; l++) {
            final long value = chunk.get(l).n;
            if (view.getRange().contains(l)) {
                assertEquals(l * l, value);
            } else {
                assertEquals(l, value);
            }
        }
    }

    @Test
    public void testIteratorFrom() {
        final Iterator<Element> it = view.listIterator(3l);

        while (it.hasNext()) {
            it.next().n = 0;
        }

        // Check only the "view" part of the chunk with index >3 iterator was
        // affected
        for (long l = 0; l < chunk.getRange().to; l++) {
            final long value = chunk.get(l).n;
            if (view.getRange().contains(l) && l >= 3) {
                assertEquals(0, value);
            } else {
                assertEquals(l, value);
            }
        }
    }

    /**
     * Creating an iterator which starts with an index out of the Chunk range should
     * throw an exception
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testIteratorFromOutOfChunkRange() {
        view.listIterator(-1l);
    }

    /**
     * Creating an iterator which starts outside the range allowed by the view
     * should throw an exception
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testIteratorFromOutOfViewRange() {
        view.listIterator(0l);
    }

    /**
     * Checks that an iterator on an empty view can be created but has no elements
     */
    @Test
    public void testIteratorOnEmptyView() {
        assertFalse(RangedListView.emptyView().iterator().hasNext());
    }

    @Test
    public void testLongSize() {
        assertEquals(4l, view.size());
        final RangedListView<Element> emptyView = RangedListView.emptyView();
        assertEquals(0l, emptyView.size());
    }

    /**
     * Checks that the element set into the view during the {@link #setUp()} method
     * is correctly replaced by calling {@link RangedListView#set(long, Object)}.
     */
    @Test
    public void testSet() {
        view.set(2, null);
        assertEquals(null, view.get(2));
        final Element e = new Element(42);
        view.set(2, e);
        assertEquals(e, view.get(2));
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

    /**
     * Setting a value outside of the view's bounds should not be allowed
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetOutOfBounds() {
        view.set(0l, null);
    }

    @Test
    public void testSetupFrom() {
        final LongRange originRange = new LongRange(2, 4);
        final Chunk<Integer> originChunk = new Chunk<>(originRange);
        for (long i = originRange.from; i < originRange.to; i++) {
            originChunk.set(i, (int) -i);
        }

        view.setupFrom(originChunk, (n) -> {
            return new Element(n);
        });

        assertEquals(1, view.get(1).n);
        assertEquals(-2, view.get(2).n);
        assertEquals(-3, view.get(3).n);
        assertEquals(4, view.get(4).n);
    }

    @Test
    public void testSize() {
        assertEquals(4, view.size());

        final RangedListView<Element> emptyView = RangedListView.emptyView();
        assertEquals(0, emptyView.size());
    }

    @Test
    public void testSubList() {
        final RangedList<Element> sub = view.subList(new LongRange(2, 4));
        assertSame(sub.size(), (long) 2);
        assertEquals(sub.get(2), view.get(2));
    }

    @Test
    public void testToArray() {
        final Object[] o = view.toArray();
        assertSame(o.length, 4);
        for (int i = 0; i < o.length; i++) {
            assertSame(elems[i + 1], o[i]);
        }
    }

    @Test
    public void testToChunk() {
        // Reminder: the view range is [1.5)
        final Chunk<Element> chunk2to4 = view.toChunk(new LongRange(2, 4));
        assertEquals(2, chunk2to4.size());
        for (int l = 2; l < 4; l++) {
            assertEquals(chunk2to4.get(l), elems[l]);
        }
    }

    /**
     * Checks that a IndexOutOfBoundsException is thrown when a range that falls
     * outside of the chunk is given.
     */
    @Test(expected = IndexOutOfBoundsException.class)
    public void testToChunkOutOfChunkBounds() {
        view.toChunk(new LongRange(-1, 6));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testToChunkOutOfViewBounds() {
        // Reminder: the view range is [1.5)
        // The next call should throw an error
        final Chunk<Element> chunk5to9 = view.toChunk(new LongRange(5, 9));
        // The code after here should not be run as a result of the exception
        // thrown just above.
        assertEquals(4, chunk5to9.size());
        for (int i = 5; i < 9; i++) {
            assertEquals(elems[i], chunk5to9.get(i));
        }
    }

    @Test
    public void testToList() {
        final ArrayList<Element> list = (ArrayList<Element>) view.toList();
        assertEquals(view.size(), list.size());
        for (int i = 0; i < list.size(); i++) {
            assertEquals(view.get(i + viewRange.from).n, list.get(i).n);
        }
    }

    @Test
    public void testToString() {
        assertEquals("[[0,0)]", RangedListView.emptyView().toString());
    }

    @Test
    public void testWriteObject() throws IOException, ClassNotFoundException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
        objectOut.writeObject(view);
        objectOut.close();

        final byte[] buf = byteOut.toByteArray();

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
        final ObjectInputStream objectIn = new ObjectInputStream(byteIn);
        objectIn.close();
        @SuppressWarnings("unchecked")
        final RangedListView<Element> r = (RangedListView<Element>) objectIn.readObject();

        assertEquals(view.size(), r.size());
    }

    @Test
    public void testWriteObjectKryo() {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final ObjectOutput objectOut = new ObjectOutput(byteOut);
        objectOut.writeObject(view);
        objectOut.writeObject(view);
        objectOut.close();

        final byte[] buf = byteOut.toByteArray();

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
        final ObjectInput objectIn = new ObjectInput(byteIn);
        @SuppressWarnings("unchecked")
        final RangedListView<Element> r1 = (RangedListView<Element>) objectIn.readObject();
        @SuppressWarnings("unchecked")
        final RangedListView<Element> r2 = (RangedListView<Element>) objectIn.readObject();
        objectIn.close();

        assertEquals(view.size(), r1.size());
        assertEquals(view.size(), r2.size());
    }
}
