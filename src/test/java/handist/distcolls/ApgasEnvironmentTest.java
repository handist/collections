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

	@Test(expected=java.lang.Exception.class)
	public void testRemoteFail() {
		finish(()-> {
			asyncAt(place(1), () -> {
				fail("Testing if a Junit fail on a remote place makes the test fail");
			});
		});
	}
}
