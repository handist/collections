package handist.distcolls;

import static org.junit.Assert.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import apgas.GlobalRuntime;

import static apgas.Constructs.*;

public class ApgasEnvironmentTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		System.setProperty("apgas.places", "2");
		GlobalRuntime.getRuntime(); // Initializes the APGAS runtime
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Test
	public void testMultipleHosts() {
		int numberOfPlaces = places().size();
		assertTrue("This test should be run with multiple hosts", numberOfPlaces > 1);
	}

	@Test()
	public void testRemoteFail() {
		finish(()-> {
			final int rootId = here().id;
			asyncAt(place(1), () -> {
				assertEquals(rootId, here().id); // Expect failure
			});
		});
	}
}
