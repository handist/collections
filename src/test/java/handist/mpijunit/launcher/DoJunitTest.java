package handist.mpijunit.launcher;

import java.io.File;

import org.junit.runners.BlockJUnit4ClassRunner;

import handist.mpijunit.ToFileRunNotifier;

public class DoJunitTest {

	public static void main(String [] args) throws Exception {
		// Obtain the class to test as an argument
		Class<?> testClass = Class.forName(args[0]);
		BlockJUnit4ClassRunner junitDefaultRunner = new BlockJUnit4ClassRunner(testClass);
		String notificationFileName = testClass.getCanonicalName() + "_0";

		String directory = null;
		if (args.length > 1) {
			directory = args[1];
		}
		File f = new File(directory, notificationFileName);

		ToFileRunNotifier notifier = new ToFileRunNotifier(f);
		junitDefaultRunner.run(notifier);	
		notifier.close();
	}
}
