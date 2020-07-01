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

public class TestBag implements Serializable {

	/** serial Version UID */
	private static final long serialVersionUID = -443049222349805678L;


	public class Element implements Serializable {
		/** Serial Version UID */
		private static final long serialVersionUID = -7271678225893322926L;
		public int n;
		public Element(int n) {
			this.n = n;
		}
		public void increase(int i) {
			n += i;
		}
		@Override
		public String toString() {
			return Integer.toString(n);
		}
	}
	
	private static final int ELEMENTS_COUNT = 6;
	
	/** bag filled with some initial members */
	private Bag<Element> bag;
	/** freshly created bag which is empty */
	private Bag<Element> newlyCreatedBag;
	/** bag include null member at the beginning and filled with some members*/
	private Bag<Element> includeNullBag;
	
	/** list include Element, each size is 3, 2, 1 */
	private List<Element> list1;
	private List<Element> list2;
	private List<Element> list3;
	
	/** list type of Element contains null, this size is 1 */
	private List<Element> nullList;
	
	/** Contains 6 initialized instances of class Element */
	private Element[] elems = new Element[ELEMENTS_COUNT];
	
	
	@Before
	public void setUp() throws Exception {
		bag = new Bag<>();
		newlyCreatedBag = new Bag<>();
		includeNullBag = new Bag<>();
		
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
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
		includeNullBag.addBag(new ArrayList<Element>(list2));	//if addBag(list2), error in method remove, removeN
		includeNullBag.addBag(new ArrayList<Element>(list3));	//multiple bags include same list, method remove is shared
	}	

	@After
	public void tearDown() throws Exception {
	}

	
	@Test
	public void testContains() {
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertTrue(bag.contains(elems[i]));
		}
		assertFalse(bag.contains(new Element(100)));
		
		assertTrue(includeNullBag.contains(null));
		for(int i = 1; i < ELEMENTS_COUNT; i++) {
			assertTrue(includeNullBag.contains(elems[i]));
		}
		assertFalse(includeNullBag.contains(new Element(100)));
		
		assertFalse(newlyCreatedBag.contains(elems[0]));
		
		bag.addBag(list1);
		assertTrue(bag.contains(elems[0]));
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
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertEquals(b.remove(), bag.remove());
		}
		
		b = includeNullBag.clone();
		assertSame(b.size(), includeNullBag.size());
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertEquals(b.remove(), includeNullBag.remove());
		}
		
		b = newlyCreatedBag.clone();
		assertSame(b.size(), 0);
	}
	
	
	@Test
	public void testAddBag() {
		Element e = new Element(-1);
		ArrayList<Element> list = new ArrayList<>(Arrays.asList(e));
		bag.addBag(list);
		assertSame(bag.size(), ELEMENTS_COUNT + 1);
		assertTrue(bag.contains(e));
		
		list = new ArrayList<>();
		bag.addBag(list);
		assertSame(bag.size(), ELEMENTS_COUNT + 1);
	}
	
	
	@Test
	public void testSize() {
		assertSame(bag.size(), ELEMENTS_COUNT);
		assertSame(includeNullBag.size(), ELEMENTS_COUNT);
		assertSame(newlyCreatedBag.size(), 0);
	}
	
	
	@Test
	public void testIsEmpty() {
		assertFalse(bag.isEmpty());
		assertFalse(includeNullBag.isEmpty());
		assertTrue(newlyCreatedBag.isEmpty());
	}
	
	
	@Test
	public void testRemove() {
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertEquals(bag.remove(), elems[ELEMENTS_COUNT - 1 - i]);
		}
		assertNull(newlyCreatedBag.remove());
	}
	
	
	@Test
	public void testRemoveN() {
		Bag<Element> b = bag.clone();
		
		List<Element> list = bag.removeN(4);
		
		assertSame(list.size(), 4);
		assertSame(bag.size(), ELEMENTS_COUNT - 4);
		for(int i = 0; i < 4; i++) {
			assertEquals(list.get(i), elems[ELEMENTS_COUNT - 1 - i]);
		}
		
		assertNull(b.removeN(100));
		
		list = includeNullBag.removeN(4);
		assertSame(list.size(), 4);
		assertSame(includeNullBag.size(), ELEMENTS_COUNT  - 4);
		
		assertNull(newlyCreatedBag.removeN(1));
	}
	
	
	@Test
	public void testIterator() {
		Iterator<Element> it = bag.iterator();
		assertTrue(it.hasNext());
		for(int i = 0; i < bag.size(); i++) {
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
	public void testForEach() {
		int[] originalValues = new int[ELEMENTS_COUNT];
		for(int i = 0; i < originalValues.length; i++) {
			originalValues[i] = elems[i].n;
		}
		
		bag.forEach(e -> e.increase(2));
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertSame(bag.remove().n, originalValues[ELEMENTS_COUNT - 1 - i] + 2);
		}
	}
	
	
	@Test
	public void testForEachConst() {
		ExecutorService exec = Executors.newFixedThreadPool(4);
		int[] originalValues = new int[ELEMENTS_COUNT];
		for(int i = 0; i < originalValues.length; i++) {
			originalValues[i] = elems[i].n;
		}
		
		bag.forEach(exec, e -> e.increase(2));
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertSame(bag.remove().n, originalValues[ELEMENTS_COUNT - 1 - i] + 2);
		}	
	}
	
	
	@Test(expected = RuntimeException.class)
	public void testForEachConstError() {
		ExecutorService exec = Executors.newFixedThreadPool(4);
		includeNullBag.forEach(exec, e -> e.increase(2));
	}
	
	
	@Test
	public void testConvertToList() {
		List<Element> list = bag.convertToList();
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertEquals(list.get(i), elems[i]);
		}
	}
	
	
	@Test
	public void testToString() {
		assertEquals(bag.toString(), "[Bag][0, 1, 2]:[3, 4]:[5]:end of Bag");
	}
	
	
	@Test
	public void testWriteObject() throws IOException, ClassNotFoundException {
		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
		ObjectOutputStream objectOut = new ObjectOutputStream(byteOut);
		objectOut.writeObject(bag);
		
		byte[] buf = byteOut.toByteArray();
		
		ByteArrayInputStream byteIn = new ByteArrayInputStream(buf);
		ObjectInputStream objectIn = new ObjectInputStream(byteIn);
		@SuppressWarnings("unchecked")
		Bag<Element> readBag = (Bag<Element>)objectIn.readObject();
		
		for(int i = 0; i < ELEMENTS_COUNT; i++) {
			assertSame(readBag.remove().n, bag.remove().n);
		}
		
		byteOut.close();
		objectOut.close();
		byteIn.close();
		objectIn.close();
		
	}
	
	
	@Test
	public void testGetReceiver() {
		Consumer<Element> c = bag.getReceiver();
		Element e = new Element(-1);
		c.accept(e);
		assertTrue(bag.contains(e));
	}

}
