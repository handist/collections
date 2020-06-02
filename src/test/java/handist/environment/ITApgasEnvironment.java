package handist.environment;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.Constructs;
import apgas.MultipleException;
import handist.mpijunit.MpiApgasTestLauncher;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;

import static apgas.Constructs.*;

/**
 * Test class that checks how the Junit various asserts behave with the APGAS
 * remote activities and illustrates how to handle them. 
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks=2, launcher=MpiApgasTestLauncher.class)
public class ITApgasEnvironment {

	/**
	 * Checks that the test is indeed run with multiple places
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
		int here = here().id;
		final int targetPlace = here == 0 ? Constructs.places().size() - 1: here-1; 
			try {
				finish(()-> {
					final int rootId = here().id;
					asyncAt(place(targetPlace), () -> {
						assertEquals(rootId, here().id); // Expect failure
					});
				});
			} catch (MultipleException e) {
				Throwable[] suppressed = e.getSuppressed();
				throw(suppressed[0]);
			}
		
	}
}
