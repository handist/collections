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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.reducer.Reducer;

public class TestBag implements Serializable {

    public class Element implements Comparable<Element>, Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = -7271678225893322926L;
        public int n;

        public Element(int n) {
            this.n = n;
        }

        @Override
        public int compareTo(Element arg0) {
            return n - arg0.n;
        }

        public void increase(int i) {
            n += i;
        }

        @Override
        public String toString() {
            return Integer.toString(n);
        }
    }

    /**
     * Reducer which keeps the "n smallest elements" from various lists given as
     * input to {@link NSmallestElement#reduce(List)}
     */
    public class NSmallestElement extends Reducer<NSmallestElement, List<Element>> {

        private static final long serialVersionUID = -1375457317873650101L;

        List<Element> nSmallest;
        int n;

        /**
         * Constructor
         *
         * @param nb number of smallest elements to keep
         */
        public NSmallestElement(int nb) {
            nSmallest = new ArrayList<>();
            n = nb;
        }

        @Override
        public void merge(NSmallestElement reducer) {
            reduce(reducer.nSmallest);
        }

        @Override
        public NSmallestElement newReducer() {
            return new NSmallestElement(n);
        }

        @Override
        public void reduce(List<Element> input) {
            nSmallest.addAll(input);
            nSmallest.sort(null);

            final int size = nSmallest.size();
            nSmallest = nSmallest.subList(0, size < n ? size : n);
        }
    }

    /**
     * Reducer which finds the smallest {@link Element} in a collection
     */
    public class SmallestElement extends Reducer<SmallestElement, Element> {

        private static final long serialVersionUID = 1L;

        Element smallest = null;

        @Override
        public void merge(SmallestElement reducer) {
            reduce(reducer.smallest);
        }

        @Override
        public SmallestElement newReducer() {
            return new SmallestElement();
        }

        @Override
        public void reduce(Element input) {
            if (smallest == null) {
                smallest = input;
            } else if (input != null && input.n < smallest.n) {
                smallest = input;
            }
        }

    }

    private static final int ELEMENTS_COUNT = 6;

    /** serial Version UID */
    private static final long serialVersionUID = -443049222349805678L;

    /** bag filled with some initial members */
    private Bag<Element> bag;
    /** Contains 6 initialized instances of class Element */
    private final Element[] elems = new Element[ELEMENTS_COUNT];
    /** bag include null member at the beginning and filled with some members */
    private Bag<Element> includeNullBag;

    /** List containing 3 {@link Element}s */
    private List<Element> list1;
    /** List containing 2 {@link Element}s */
    private List<Element> list2;
    /** List containing 1 {@link Element}s */
    private List<Element> list3;

    /** freshly created bag which is empty */
    private Bag<Element> newlyCreatedBag;

    /** list type of Element contains null, this size is 1 */
    private List<Element> nullList;

    @Before
    public void setUp() throws Exception {
        bag = new Bag<>();
        newlyCreatedBag = new Bag<>();
        includeNullBag = new Bag<>();

        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            elems[i] = new Element(i);
        }

        list1 = new ArrayList<>(Arrays.asList(elems[0], elems[1], elems[2]));
        list2 = new ArrayList<>(Arrays.asList(elems[3], elems[4]));
        list3 = new ArrayList<>(Arrays.asList(elems[5]));
        nullList = new ArrayList<>(Arrays.asList(null, elems[1], elems[2]));

        bag.addBag(list1);
        bag.addBag(list2);
        bag.addBag(list3);
        includeNullBag.addBag(nullList);
        includeNullBag.addBag(new ArrayList<>(list2)); // if addBag(list2), error in method remove, removeN
        includeNullBag.addBag(new ArrayList<>(list3)); // multiple bags include same list, method remove is
        // shared
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAddBag() {
        final Element e = new Element(-1);
        ArrayList<Element> list = new ArrayList<>(Arrays.asList(e));
        bag.addBag(list);
        assertSame(bag.size(), ELEMENTS_COUNT + 1);
        assertTrue(bag.contains(e));

        list = new ArrayList<>();
        bag.addBag(list);
        assertSame(bag.size(), ELEMENTS_COUNT + 1);
    }

    @Test
    public void testClear() {
        assertFalse(bag.isEmpty());
        bag.clear();
        assertTrue(bag.isEmpty());
    }

    @Test
    public void testClone() {
        Bag<?> b = bag.clone();
        assertSame(b.size(), bag.size());
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertEquals(b.remove(), bag.remove());
        }

        b = includeNullBag.clone();
        assertSame(b.size(), includeNullBag.size());
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertEquals(b.remove(), includeNullBag.remove());
        }

        b = newlyCreatedBag.clone();
        assertSame(b.size(), 0);
    }

    @Test
    public void testContains() {
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertTrue(bag.contains(elems[i]));
        }
        assertFalse(bag.contains(new Element(100)));

        assertTrue(includeNullBag.contains(null));
        for (int i = 1; i < ELEMENTS_COUNT; i++) {
            assertTrue(includeNullBag.contains(elems[i]));
        }
        assertFalse(includeNullBag.contains(new Element(100)));

        assertFalse(newlyCreatedBag.contains(elems[0]));

        bag.addBag(list1);
        assertTrue(bag.contains(elems[0]));
    }

    @Test
    public void testConvertToList() {
        final List<Element> list = bag.convertToList();
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertEquals(list.get(i), elems[i]);
        }
    }

    @Test
    public void testForEach() {
        final int[] originalValues = new int[ELEMENTS_COUNT];
        for (int i = 0; i < originalValues.length; i++) {
            originalValues[i] = elems[i].n;
        }

        bag.forEach(e -> e.increase(2));
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertSame(bag.remove().n, originalValues[ELEMENTS_COUNT - 1 - i] + 2);
        }
    }

    @Test
    public void testForEachConst() {
        final ExecutorService exec = Executors.newFixedThreadPool(4);
        final int[] originalValues = new int[ELEMENTS_COUNT];
        for (int i = 0; i < originalValues.length; i++) {
            originalValues[i] = elems[i].n;
        }

        bag.forEach(exec, e -> e.increase(2));
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertSame(originalValues[ELEMENTS_COUNT - 1 - i] + 2, bag.remove().n);
        }
    }

    @Test(expected = RuntimeException.class)
    public void testForEachConstError() {
        final ExecutorService exec = Executors.newFixedThreadPool(4);
        includeNullBag.forEach(exec, e -> e.increase(2));
    }

    @Test
    public void testGetReceiver() {
        final Consumer<Element> c = bag.getReceiver();
        final Element e = new Element(-1);
        c.accept(e);
        assertTrue(bag.contains(e));
    }

    @Test
    public void testIsEmpty() {
        assertFalse(bag.isEmpty());
        assertFalse(includeNullBag.isEmpty());
        assertTrue(newlyCreatedBag.isEmpty());
    }

    @Test
    public void testIterator() {
        Iterator<Element> it = bag.iterator();
        assertTrue(it.hasNext());
        for (int i = 0; i < bag.size(); i++) {
            assertEquals(it.next(), elems[i]);
        }

        bag.clear();
        assertFalse(it.hasNext());

        it = newlyCreatedBag.iterator();
        assertFalse(it.hasNext());
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testIteratorError() {
        newlyCreatedBag.iterator().next();
    }

    @Test
    public void testParallelForEach() {
        final int[] originalValues = new int[ELEMENTS_COUNT];
        for (int i = 0; i < originalValues.length; i++) {
            originalValues[i] = elems[i].n;
        }

        bag.parallelForEach(e -> e.increase(2));
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertSame(originalValues[ELEMENTS_COUNT - 1 - i] + 2, bag.remove().n);
        }
    }

    @Test
    public void testParallelReduce() {
        final SmallestElement smallest = new SmallestElement();
        final SmallestElement result = bag.parallelReduce(smallest);

        assertEquals(smallest, result);
        assertEquals(smallest.smallest, elems[0]);
    }

    @Test
    public void testParallelReduceList() {
        final int nbOfElementsToKeep = 3;
        final NSmallestElement reducer = new NSmallestElement(nbOfElementsToKeep);
        bag.parallelReduceList(reducer);

        assertEquals("Expected reducer to contain " + nbOfElementsToKeep + " elements", nbOfElementsToKeep,
                reducer.nSmallest.size());
        for (int i = 0; i < nbOfElementsToKeep; i++) {
            assertEquals(reducer.nSmallest.get(i), elems[i]);
            assertEquals(reducer.nSmallest.get(i).n, i);
        }
    }

    @Test
    public void testReduce() {
        final SmallestElement smallest = new SmallestElement();
        final SmallestElement result = bag.reduce(smallest);

        assertEquals(smallest, result);
        assertEquals(smallest.smallest, elems[0]);
    }

    @Test
    public void testReduceList() {
        final int nbOfElementsToKeep = 3;
        final NSmallestElement reducer = new NSmallestElement(nbOfElementsToKeep);
        bag.reduceList(reducer);

        assertEquals("Expected reducer to contain " + nbOfElementsToKeep + " elements", nbOfElementsToKeep,
                reducer.nSmallest.size());
        for (int i = 0; i < nbOfElementsToKeep; i++) {
            assertEquals(reducer.nSmallest.get(i), elems[i]);
            assertEquals(reducer.nSmallest.get(i).n, i);
        }
    }

    @Test
    public void testRemove() {
        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertEquals(bag.remove(), elems[ELEMENTS_COUNT - 1 - i]);
        }
        assertNull(newlyCreatedBag.remove());
    }

    @Test
    public void testRemoveN() {
        List<Element> list = bag.remove(4);
        assertSame(list.size(), 4);
        assertSame(bag.size(), ELEMENTS_COUNT - 4);
        for (final Element e : list) {
            assertFalse(bag.contains(e));
        }

        // Trying to remove more than is contained in the bag returns everything
        final int size = bag.size();
        assertTrue(size < 4);
        assertFalse(bag.isEmpty());
        list = bag.remove(4);
        assertEquals(size, list.size());
        assertTrue(bag.isEmpty());

        // Trying to remove from an empty bag returns an empty list
        assertTrue(bag.remove(1).isEmpty());

        // Checking that having a null element does not bother the method.
        list = includeNullBag.remove(4);
        assertSame(list.size(), 4);
        assertSame(includeNullBag.size(), ELEMENTS_COUNT - 4);
        assertTrue(newlyCreatedBag.remove(1).isEmpty());
    }

    @Test
    public void testSize() {
        assertSame(bag.size(), ELEMENTS_COUNT);
        assertSame(includeNullBag.size(), ELEMENTS_COUNT);
        assertSame(newlyCreatedBag.size(), 0);
    }

    @Test
    public void testToString() {
        assertEquals(bag.toString(), "[Bag][0, 1, 2]:[3, 4]:[5]:end of Bag");
    }

    @Test
    public void testWriteObject() throws IOException, ClassNotFoundException {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
        objectOut.writeObject(bag);
        objectOut.close();

        final byte[] buf = byteOut.toByteArray();

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
        final ObjectInputStream objectIn = new ObjectInputStream(byteIn);
        @SuppressWarnings("unchecked")
        final Bag<Element> readBag = (Bag<Element>) objectIn.readObject();

        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertSame(readBag.remove().n, bag.remove().n);
        }

        objectIn.close();
    }

    @Test
    public void testWriteObjectKryo() {
        final ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        final ObjectOutput objectOut = new ObjectOutput(byteOut);
        objectOut.writeObject(bag);

        objectOut.close();

        final byte[] buf = byteOut.toByteArray();

        final ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
        final ObjectInput objectIn = new ObjectInput(byteIn);
        @SuppressWarnings("unchecked")
        final Bag<Element> readBag = (Bag<Element>) objectIn.readObject();

        for (int i = 0; i < ELEMENTS_COUNT; i++) {
            assertSame(readBag.remove().n, bag.remove().n);
        }

        objectIn.close();
    }

}
