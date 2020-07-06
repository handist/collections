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

import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import org.junit.Before;
import org.junit.Test;

public class TestLongRange {

	LongRange range0to10;
	LongRange range0to5;
	LongRange range5;

	@Before
	public void setUp() throws Exception {
		range0to10 = new LongRange(0l, 10l);
		range0to5 = new LongRange(0l, 5l);
		range5 = new LongRange(5l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testLongRange() {
		new LongRange(10,5);
	}

	@Test
	public void testSize() {
		assertEquals(10l, range0to10.size());
		assertEquals(0l, range5.size());
	}

	@Test
	public void testContains() {
		assertFalse(range0to10.contains(-1l));
		assertFalse(range0to10.contains(10l));
		for (long l = 0; l < 10; l++) {
			assertTrue(range0to10.contains(l));
		}

		// A single point range does not contain anything
		for (long l = 0; l < 10; l++) {
			assertFalse(range5.contains(l));
		}
	}

	@Test
	public void testContainsLongRange() {
		assertFalse(range5.contains(range0to10));
		assertTrue(range0to10.contains(range5));
		assertTrue(range0to10.contains(range0to10));
		assertTrue(range5.contains(range5));

		LongRange range0to5 = new LongRange(0l, 5l);
		assertFalse(range0to5.contains(range0to10));
	}

	@Test
	public void testIsOverlapped() {
		LongRange range10to20 = new LongRange(10l, 20l);
		LongRange range5to20 = new LongRange(5l,20l);

		// Cases with LongRange with identical bounds
		assertTrue(range5.isOverlapped(range5));
		assertTrue(range5.isOverlapped(range0to10));
		assertTrue(range0to10.isOverlapped(range5));
		assertTrue(range5.isOverlapped(range5to20));
		assertTrue(range5to20.isOverlapped(range5));
		assertFalse(range5.isOverlapped(range0to5));
		assertFalse(range0to5.isOverlapped(range5));
		assertFalse(range5.isOverlapped(range10to20));
		assertFalse(range10to20.isOverlapped(range5));

		// Other cases with "normal" LongRange instancecs
		assertTrue(range0to5.isOverlapped(range0to10));
		assertTrue(range0to10.isOverlapped(range0to5));
		assertFalse(range0to10.isOverlapped(range10to20));
		assertFalse(range10to20.isOverlapped(range0to10));
		assertTrue(range0to10.isOverlapped(range5to20));
		assertTrue(range5to20.isOverlapped(range0to10));
	}

	@Test
	public void testForEachLongConsumer() {
		ArrayList<Long> collector = new ArrayList<>();
		range5.forEach((LongConsumer)(l)-> collector.add(l));
		assertTrue(collector.isEmpty());

		range0to10.forEach((LongConsumer)(l)-> collector.add(l));
		for (long l = 0; l < 10; l++) {
			assertSame(l,collector.remove(0));
		}
	}

	@Test
	public void testStream() {
		ArrayList<Long> collector = new ArrayList<>();
		range5.stream().forEach((l)->collector.add(l));
		assertTrue(collector.isEmpty());

		range0to10.stream().forEach((l)->collector.add(l));
		for (long l = 0; l < 10; l++) {
			assertSame(l,collector.remove(0));
		}
	}

	@Test
	public void testCompareTo() {
		LongRange range0 = new LongRange(0l);
		LongRange range10to20 = new LongRange(10l,20l);

		assertSame(range5.compareTo(range0to10), -range0to10.compareTo(range5));
		assertEquals(1, range5.compareTo(range0to10));
		assertEquals(1, range0to10.compareTo(range0));
		assertEquals(-1, range0.compareTo(range0to10));
		assertEquals(-1, range0to5.compareTo(new LongRange(0,7)));
		assertEquals(-1, range0to10.compareTo(range10to20));
		assertEquals(0, range5.compareTo(range5));
		assertEquals(0, range0to10.compareTo(range0to10));
	}

	@Test(expected=NullPointerException.class)
	public void testCompareToNullArgument() {
		range5.compareTo(null);
	}

	@Test
	public void testToString() {
		assertTrue(range5.toString().equals("[5,5)"));
		assertTrue(range0to10.toString().equals("[0,10)"));
	}

	@SuppressWarnings("unlikely-arg-type")
	@Test
	public void testEquals() {
		assertFalse(range0to10.equals(new Long(5)));
		assertTrue(range5.equals(range5));
		assertFalse(range5.equals(range0to10));
		assertFalse(range0to10.equals(range5));
		assertFalse(range0to10.equals(range0to5));
	}

	@Test
	public void testHashCode() {
		int firstHash0to10 = range0to10.hashCode();
		int firstHash0to5 = range0to5.hashCode();
		int firstHash5 = range5.hashCode();

		//Do a little stuff
		for (long l = 0 ; l < 42; l++);

		int secondHash0to10 = range0to10.hashCode();
		int secondHash0to5 = range0to5.hashCode();
		int secondHash5 = range5.hashCode();

		//Check that the hash is the same
		assertSame(firstHash0to10, secondHash0to10);
		assertSame(firstHash0to5, secondHash0to5);
		assertSame(firstHash5, secondHash5);
	}


	@Test 
	public void testIterator() {
		assertFalse(range5.iterator().hasNext());

		ArrayList<Long> collector = new ArrayList<>();
		for (Long l : range0to10) {
			collector.add(l);
		}

		for (long l = 0l; l < 10l; l++) {
			assertSame(l,collector.remove(0));
		}
	}

	@Test
	public void testSplit() {
		List<LongRange> result = range5.split(1);
		assertEquals(1, result.size());
		assertEquals(range5, result.remove(0));

		result = range0to10.split(2);
		assertEquals(2, result.size());
		assertEquals(range0to5, result.get(0));
		assertEquals(new LongRange(5l,10l), result.get(1));

		result = range0to5.split(3);
		assertEquals(3, result.size());
		assertEquals(new LongRange(0l,2l), result.get(0));
		assertEquals(new LongRange(2l,4l), result.get(1));
		assertEquals(new LongRange(4l,5l), result.get(2));
	}

	@Test
	public void testSplitList() {
		List<LongRange> input = new ArrayList<>();
		List<List<LongRange>> output;

		//Test with a single LongRange
		input.add(range0to10);
		output = LongRange.splitList(2, input);
		assertEquals(2, output.size());
		List<LongRange> firstList = output.remove(0);
		List<LongRange> secondList = output.remove(0);
		assertEquals(1, firstList.size());
		assertEquals(1, secondList.size());

		assertEquals(range0to5, firstList.remove(0));
		assertEquals(new LongRange(5, 10), secondList.remove(0));

		input.clear();
		//Test with multiple LongRanges
		input.add(range0to5);
		input.add(new LongRange(5, 10));
		input.add(new LongRange(10, 20));

		output = LongRange.splitList(3, input);
		assertEquals(3, output.size());
		firstList = output.remove(0);
		secondList = output.remove(0);
		List<LongRange> thirdList = output.remove(0);

		assertEquals(2, firstList.size());
		assertEquals(2, secondList.size());
		assertEquals(1, thirdList.size());

		assertEquals(range0to5, firstList.remove(0));
		assertEquals(new LongRange(5,7), firstList.remove(0));

		assertEquals(new LongRange(7,10), secondList.remove(0));
		assertEquals(new LongRange(10,14), secondList.remove(0));

		assertEquals(new LongRange(14, 20), thirdList.remove(0));
	}
}
