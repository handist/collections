package handist.mpijunit.launcher;

import handist.collections.mpi.MPILauncher;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * This class is used to transparently call the regular {@link MPILauncher} with the main class
 * as {@link DoJunitTest} and the arguments received. 
 * 
 * @author Patrick Finnerty
 *
 */
public class TestLauncher {

	public static void main(String [] args) throws Exception {
		//Insert the desired main in the arguments
		String [] newArgs = new String[args.length + 1];
		int newIndex = 0;
		int oldIndex = 0;

		// If using MPJ, we put the first three arguments
		if (args.length > 2) { 
			while(newIndex < 3) {
				newArgs[newIndex++] = args[oldIndex++];
			}
		}
		
		newArgs[newIndex++] = DoJunitTest.class.getCanonicalName();
		
		while (oldIndex < args.length) {
			newArgs[newIndex++] = args[oldIndex++];
		}
		
		// Call TeamedPlaceGroup setup method to add its plugin to the MPILauncher
		TeamedPlaceGroup.setup();
		
		// Call the MPILauncher with the modified arguments
		MPILauncher.main(newArgs);
	}
}
