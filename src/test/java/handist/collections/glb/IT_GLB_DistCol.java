package handist.collections.glb;

import static apgas.Constructs.at;
import static handist.collections.glb.GlobalLoadBalancer.underGLB;
import static handist.collections.glb.Util.genRandStr;
import static handist.collections.glb.Util.makePrefixTest;
import static handist.collections.glb.Util.makeSuffixTest;
import static handist.collections.glb.Util.printExceptionAndThrowFirst;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.Serializable;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_GLB_DistCol implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = 3890454865986201964L;

	/** Number of ranges to populate this collection */
	final static long LONGRANGE_COUNT = 20l;
	/** Size of individual ranges */
	final static long RANGE_SIZE = 20l;

	/** Total number of elements contained in the {@link DistCol} */
	final static long DATA_SIZE = LONGRANGE_COUNT * RANGE_SIZE;

	/**
	 * Helper method which fill the provided DistCol with values
	 * 
	 * @param col the collection which needs to be populated
	 */
	private static void y_populateDistCol(DistCol<Element> col) {
		for (long l = 0l; l < LONGRANGE_COUNT; l++) {
			long from = l * RANGE_SIZE;
			long to = from + RANGE_SIZE;
			String lrPrefix = "LR[" + from + ";" + to + "]";
			LongRange lr = new LongRange(from, to);
			Chunk<Element> c = new Chunk<>(lr);
			for (long i = from; i < to; i++) {
				String value = genRandStr(lrPrefix + ":" + i + "#");
				c.set(i, new Element(value));
			}
			col.add(c);
		}
	}

	/**
	 * Checks that the prefix of each element in {@link #distCol} is the one
	 * specified
	 * 
	 * @param prefix expected prefix
	 * @throws Throwable if thrown during the check
	 */
	private static void z_checkPrefixIs(DistCol<Element> col, final String prefix) throws Throwable {
		try {
			col.GLOBAL.forEach((e) -> assertTrue("String was " + e.s, e.s.startsWith(prefix)));
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}
	}

	/**
	 * Checks that the suffix of every element in the collection is the one
	 * specified
	 * 
	 * @param suffix string which should be at the end of each element
	 * @throws Throwable of throw during the test
	 */
	private static void z_checkSuffixIs(DistCol<Element> col, final String suffix) throws Throwable {
		try {
			col.GLOBAL.forEach((e) -> assertTrue("String was " + e.s, e.s.endsWith(suffix)));
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}
	}

	/**
	 * Checks that the distCol contains exactly the specified number of entries. The
	 * {@link DistCol#size()} needs to match the specified parameter.
	 * 
	 * @param expectedCount expected total number of entries in {@link #distCol}
	 * @throws Throwable if thrown during the check
	 */
	private static void z_checkTotalElements(DistCol<Element> col, long expectedCount) throws Throwable {
		long count = 0;
		for (Place p : col.placeGroup().places()) {
			count += at(p, () -> {
				return col.size();
			});
		}
		assertEquals(expectedCount, count);
	}

	/**
	 * Distributed collection which is the object of the tests. It is defined on the
	 * entire world.
	 */
	DistCol<Element> distCol;

	/**
	 * Whole world
	 */
	TeamedPlaceGroup placeGroup;

	@Before
	public void setUp() throws Exception {
		placeGroup = TeamedPlaceGroup.getWorld();
		distCol = new DistCol<>();

		y_populateDistCol(distCol);
	}

	@After
	public void tearDown() throws Exception {
		distCol.destroy();
	}

	@Test(timeout = 20000)
	public void testForEach() throws Throwable {
		try {
			ArrayList<Exception> ex = underGLB(() -> {
				distCol.GLB.forEach(makePrefixTest);
			});
			if (!ex.isEmpty()) {
				ex.get(0).printStackTrace();
				throw ex.get(0);
			}
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}
		z_checkTotalElements(distCol, DATA_SIZE);
		z_checkPrefixIs(distCol, "Test");
	}
	
	/**
	 * This test checks that the map function of the {@link DistColGlb} handle produces the expected results. 
	 * It is currently ignored due to a pending problem in the implementation.
	 * @throws Throwable if thrown during the test
	 * @see DistColGlb#map(handist.collections.function.SerializableFunction)
	 */
	@Ignore
	@Test(timeout = 20000)
	public void testMap() throws Throwable {
		try {
			ArrayList<Exception> ex = underGLB(() -> {
				DistCol<Element> result = distCol.GLB.map((e)->{return new Element(e.s + "Test");}).result();
				
				try {
					z_checkTotalElements(distCol, DATA_SIZE); // This shouldn't have changed
					z_checkTotalElements(result, DATA_SIZE); // Should contain the same number of elements
					z_checkSuffixIs(result, "Test"); // The elements contained in the result should have 'Test' as prefix
				} catch (Throwable e) {
					throw new RuntimeException(e);
				}
				
				
			});
			if (!ex.isEmpty()) {
				ex.get(0).printStackTrace();
				throw ex.get(0);
			}
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}
	}

	@Test(timeout = 40000)
	public void testTwoForEachAfterOneAnother() throws Throwable {
		try {
			ArrayList<Exception> ex = underGLB(() -> {
				DistFuture<?> prefixFuture = distCol.GLB.forEach(makePrefixTest);
				distCol.GLB.forEach(makeSuffixTest).after(prefixFuture);
			});
			if (!ex.isEmpty()) {
				ex.get(0).printStackTrace();
				throw ex.get(0);
			}
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}
		z_checkTotalElements(distCol, DATA_SIZE);
		z_checkPrefixIs(distCol, "Test");
		z_checkSuffixIs(distCol, "Test");
	}

	@Test(timeout = 40000)
	public void testTwoConcurrentForEach() throws Throwable {
		try {
			ArrayList<Exception> ex = underGLB(() -> {
				distCol.GLB.forEach(makePrefixTest);
				distCol.GLB.forEach(makeSuffixTest);
			});
			if (!ex.isEmpty()) {
				ex.get(0).printStackTrace();
				throw ex.get(0);
			}
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}
		z_checkTotalElements(distCol, DATA_SIZE);
		z_checkPrefixIs(distCol, "Test");
		z_checkSuffixIs(distCol, "Test");
	}

	@Test(timeout = 40000)
	public void testTwoDifferentCollectionsComputations() throws Throwable {
		DistCol<Element> otherCol = new DistCol<>();
		y_populateDistCol(otherCol);
		try {
			ArrayList<Exception> ex = underGLB(() -> {
				distCol.GLB.forEach(makeSuffixTest);
				otherCol.GLB.forEach(makePrefixTest);
			});
			if (!ex.isEmpty()) {
				ex.get(0).printStackTrace();
				throw ex.get(0);
			}
		} catch (MultipleException me) {
			printExceptionAndThrowFirst(me);
		}

		z_checkTotalElements(distCol, DATA_SIZE);
		z_checkTotalElements(otherCol, DATA_SIZE);
		z_checkSuffixIs(distCol, "Test");
		z_checkPrefixIs(otherCol, "Test");

		otherCol.destroy();
	}
}
