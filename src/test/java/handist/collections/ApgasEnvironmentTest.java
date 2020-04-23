package handist.collections;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import apgas.GlobalRuntime;
import apgas.MultipleException;

import static apgas.Constructs.*;

/**
 * Test class that checks how the Junit various asserts behave with the APGAS
 * remote activities and illustrates how to handle them. 
 */
public class ApgasEnvironmentTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.setProperty("apgas.places", "2");
		GlobalRuntime.getRuntime(); // Initializes the APGAS runtime
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	/**
	 * Checks that the test was indeed run with multiple places
	 */
	@Test
	public void testMultipleHosts() {
		int numberOfPlaces = places().size();
		assertTrue("This test should be run with multiple hosts", numberOfPlaces > 1);
	}

	/**
	 * Tests the failure of an assertEquals call in an asynchronous remote
	 * remote activity.
	 */
	@Test(expected=java.lang.AssertionError.class)
	public void testRemoteAssertEqualsFailure() throws Throwable {
		try {
			finish(()-> {
				final int rootId = here().id;
				asyncAt(place(1), () -> {
					assertEquals(rootId, here().id); // Expect failure
				});
			});
		} catch (MultipleException e) {
			Throwable[] suppressed = e.getSuppressed();
			throw(suppressed[0]);
		}
	}
}
