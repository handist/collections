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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test default methods of RangedList.
 */
public class TestRangedList {

    /** Element class for RangedList */
    private class Element {
        public int n;

        public Element(int n) {
            this.n = n;
        }
    }

    /**
     * Test class extends RangedList. Implement only abstract methods.
     */
    private class TmpRangedList<T> extends RangedList<T> {

        private final Chunk<T> chunk;

        public TmpRangedList(LongRange range) {
            chunk = new Chunk<>(range);
        }

        @Override
        public RangedList<T> cloneRange(LongRange range) {
            return chunk.cloneRange(range);
        }

        @Override
        public boolean contains(Object o) {
            return chunk.contains(o);
        }

        @Override
        public T get(long index) {
            return chunk.get(index);
        }

        @Override
        public LongRange getRange() {
            return chunk.getRange();
        }

        @Override
        public Iterator<T> iterator() {
            return chunk.iterator();
        }

        @Override
        public T set(long index, T value) {
            return chunk.set(index, value);
        }

        @Override
        public <S> void setupFrom(RangedList<S> source, Function<? super S, ? extends T> func) {
            chunk.setupFrom(source, func);

        }

        @Override
        public Object[] toArray() {
            return chunk.toArray();
        }

        @Override
        public Object[] toArray(LongRange r) {
            return chunk.toArray(r);
        }

        @Override
        public Chunk<T> toChunk(LongRange r) {
            return chunk.toChunk(r);
        }

        @Override
        public List<T> toList(LongRange r) {
            return chunk.toList(r);
        }
    }

    /** Contains 10 initialized instances of class Element */
    private Element[] elems;

    private LongRange range;
    /** RangedList filled with 10 initial members */
    private TmpRangedList<Element> rangedList;

    @Before
    public void setup() {
        elems = new Element[10];
        range = new LongRange(0, 10);
        rangedList = new TmpRangedList<>(range);

        for (int i = 0; i < 10; i++) {
            elems[i] = new Element(i);
            rangedList.set(i, elems[i]);
        }
    }

    @Test
    public void testContainsAll() {
        final ArrayList<Element> list = new ArrayList<>();
        list.add(elems[4]);
        list.add(elems[8]);
        assertTrue(rangedList.containsAll(list));

        list.add(new Element(-1));
        assertFalse(rangedList.containsAll(list));

        list.add(null);
        assertFalse(rangedList.containsAll(list));
    }

    @Test
    public void testEquals() {
        final TmpRangedList<Element> r = new TmpRangedList<>(range);
        for (int i = 0; i < elems.length; i++) {
            r.set(i, elems[i]);
        }
        assertFalse(rangedList.equals(r));

        assertFalse(rangedList.equals(null));
        assertFalse(rangedList.equals(new Element(-1)));
    }

    @Test
    public void testHashCode() {
        final int firstHash = rangedList.hashCode();
        // Do a little stuff
        for (int i = 0; i < 43; i++) {
            ;
        }
        final int secondHash = rangedList.hashCode();
        // Check that the hash is the same
        assertEquals(firstHash, secondHash);
    }

    @Ignore
    @Test
    public void testIsEmpty() {
        assertFalse(rangedList.isEmpty());
        rangedList = new TmpRangedList<>(new LongRange(0, 10));
        assertTrue(rangedList.isEmpty());
    }

    @Test
    public void testMap() {
        final RangedList<Element> r = rangedList.map((e) -> {
            return new Element(-e.n);
        });

        for (int i = 0; i < r.size(); i++) {
            assertEquals(-i, r.get(i).n);
        }
    }

    @Test
    public void testRangeCheck() {
        rangedList.rangeCheck(0l);
        rangedList.rangeCheck(9l);
        rangedList.rangeCheck(new LongRange(1, 8));

        try {
            rangedList.rangeCheck(10l);
            throw new RuntimeException("index 10 is not contained, but rangeCheck does not throw an exception.");
        } catch (final IndexOutOfBoundsException e) {
        }

        try {
            rangedList.rangeCheck(null);
            throw new RuntimeException(
                    "index null is not the correct operation, but rangeCheck does not throw an exception.");
        } catch (final NullPointerException e) {
        }
    }

    @Test
    public void testSize() {
        assertEquals(10, rangedList.size());
    }

    @Ignore
    @Test
    public void testSplitRange() {
        // WARNING :generate List<RangedListView>, but RangedListView can not be
        // generated by other than Chunk or RangedListView.
        List<RangedList<Element>> split = rangedList.splitRange(2l, 5l);
        assertEquals(3, split.size());
        assertEquals(new LongRange(0, 2), split.get(0).getRange());
        assertEquals(new LongRange(2, 5), split.get(1).getRange());
        assertEquals(new LongRange(5, 10), split.get(2).getRange());

        split = rangedList.splitRange(-2l, 12l);
    }

    @Ignore
    @Test
    public void testSubList() {
        // WARNING :generate List<RangedListView>, but RangedListView can not be
        // generated by other than Chunk or RangedListView.
    }

    @Test
    public void testToChunk() {
        final Chunk<Element> chunk = rangedList.toChunk();
        assertEquals(10, chunk.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(elems[i], chunk.get(i));
        }
    }

    @Test
    public void testToList() {
        final List<Element> list = rangedList.toList();
        assertEquals(10, list.size());
        for (int i = 0; i < 10; i++) {
            assertEquals(elems[i], list.get(i));
        }
    }
}
