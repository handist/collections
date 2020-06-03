package handist.collections.mpi;

import static apgas.Constructs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import apgas.Configuration;
import apgas.GlobalRuntime;
import apgas.impl.Config;
import apgas.impl.Launcher;
import handist.collections.dist.TeamedPlaceGroup;
import mpi.MPI;
import mpi.MPIException;


/**
 * The {@link ApgasMPILauncher} class implements a launcher for the apgas runtime using MPI and setup TeamedPlaceGroup.
 * Programs should be run in the following way:
 * <p>
 * <em>mpirun -np <nb hosts> -host <, seperated list of hosts> java -cp <: seperated list of jar> apgas.collections.mpi.ApgasMPILauncher {-verbose} { params for MPI.init() -body} <class with main to be launched on place 0> <arguments for said class></em>
 * <p>
 * For instance, to launch the HelloWorld example with message "Hello" on 4 servers, a solution would be:
 * <p>
 * <ul>
 * <li><em>mpirun -np 4 -host piccolo04,piccolo05,piccolo06,piccolo07 java 
 * -cp /home/userA/apgaslibs/*:handist.jar:YourClassDir -Djava.library.path=/home/userA/MPJ/lib 
 * handist.collections.mpi.ApgasMPILauncher handist.collections.examples.HelloMPICluster msg</em></li>
 * <li><em>mpirun -np 4 -host piccolo04,piccolo05,piccolo06,piccolo07 java -cp ...  -Djava.library.path=... handist.collections.mpi.ApgasMPILauncher -verbose 0 0 native others -body handist.collections.examples.HelloMPICluster msg</em></li>
 * </ul>
 *
 *
 * @author Toshiyuki (original MPILauncher), Tomio Kamada (TeamedPlaceGroup setup)
 */

final public class ApgasMPILauncher implements Launcher {
  /** Identifier of this place */
  static int commRank;
  /** Number of places in the system */    
  static int commSize;

  /**
   * Set in the main method according to the value set by {@link Configuration#APGAS_VERBOSE_LAUNCHER}
   */
  static boolean verboseLauncher;

  /**
   * Constructs a new {@link ApgasMPILauncher} instance.
   */
  public ApgasMPILauncher() {
  }

  /**
   * Launches one process with the given command line at the specified host.
   *
   * @param command command line
   * @param host host
   * @param verbose dumps the executed commands to stderr
   * @return the process object
   * @throws Exception
   *           if launching fails
   */
  @Override
  public Process launch(List<String> command, String host, boolean verbose)
      throws Exception {

    System.err.println("[ApgasMPILauncher] Internal Error");
    MPI.Finalize();
    System.exit(-1);

    return null;
  }

  /**
   * Launches n processes with the given command line and host list. The first
   * host of the list is skipped. If the list is incomplete, the last host is
   * repeated.
   *
   * @param n number of processes to launch
   * @param command command line
   * @param hosts host list (not null, not empty, but possibly incomplete)
   * @param verbose dumps the executed commands to stderr
   * @throws Exception if launching fails
   */
  @Override
  public void launch(int n, List<String> command, List<String> hosts,
      boolean verbose) throws Exception {

    if (n + 1 != commSize) {
      System.err.println("[ApgasMPILauncher] " + Configuration.APGAS_PLACES
          + " should be equal to number of MPI processes " + commSize);
      MPI.Finalize();
      System.exit(-1);
    }

    final byte[] baCommand = serializeToByteArray(command.toArray(new String[command.size()]));
    final int[] msglen = new int[1];
    msglen[0] = baCommand.length;
    // MPI.COMM_WORLD.Bcast(msglen, 1, MPI.INT, 0);
    MPI.COMM_WORLD.Bcast(msglen, 0, 1, MPI.INT, 0);
    // MPI.COMM_WORLD.Bcast(baCommand, msglen[0], MPI.BYTE, 0);
    MPI.COMM_WORLD.Bcast(baCommand, 0, msglen[0], MPI.BYTE, 0);

  }

  static void slave() throws Exception {

    final int[] msglen = new int[1];
    MPI.COMM_WORLD.Bcast(msglen, 0, 1, MPI.INT, 0);
    final byte[] msgbuf = new byte[msglen[0]];
    MPI.COMM_WORLD.Bcast(msgbuf, 0, msglen[0], MPI.BYTE, 0);
    final String[] command = (String[]) deserializeFromByteArray(msgbuf);

    for (int i = 1; i < command.length; i++) {
      final String term = command[i];
      if (term.startsWith("-D")) {
        final String[] kv = term.substring(2).split("=", 2);
        if(verboseLauncher) {
	    System.err.println("[" + commRank + "] setProperty \"" + kv[0]
            + "\" = \"" + kv[1] + "\"");
	}
        System.setProperty(kv[0], kv[1]);
      }
    }

    GlobalRuntime.getRuntime();
  }

  /**
   * Shuts down the {@link Launcher} instance.
   */
  @Override
  public void shutdown() {
    try {
      MPI.Finalize();
    } catch (final MPIException e) {
      e.printStackTrace();
    }
    System.exit(0);
  }

  /**
   * Checks that all the processes launched are healthy.
   *
   * @return true if all subprocesses are healthy
   */
  @Override
  public boolean healthy() {
    return true;
  }

  static int[] stringToIntArray(String src) {
    final char[] charArray = src.toCharArray();
    final int[] intArray = new int[charArray.length];
    for (int i = 0; i < charArray.length; i++) {
      intArray[i] = charArray[i];
    }
    return intArray;
  }

  static String intArrayToString(int[] src) {
    final char[] charArray = new char[src.length];
    for (int i = 0; i < src.length; i++) {
      charArray[i] = (char) src[i];
    }
    final String str = new String(charArray);
    return str;
  }

  static byte[] serializeToByteArray(Serializable obj) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    return baos.toByteArray();
  }

  static Object deserializeFromByteArray(byte[] barray)
      throws IOException, ClassNotFoundException {
    final ByteArrayInputStream bais = new ByteArrayInputStream(barray);
    final ObjectInputStream ois = new ObjectInputStream(bais);
    return ois.readObject();
  }

  public static void main(String[] args) throws Exception {

    verboseLauncher = Boolean.parseBoolean(System.getProperty(Configuration.APGAS_VERBOSE_LAUNCHER, "false"));
    String[] mpiArgs = new String[0];
    ArrayList<String> bag = new ArrayList<>();
    for(int i = 0; i<args.length; i++) {
	if(mpiArgs.length==0 && args[i].equals("-verbose")) {
	    verboseLauncher = true;
	} else if(args[i].equals("-body")) {
            mpiArgs = bag.toArray(mpiArgs);
            bag.clear();
        } else {
            bag.add(args[i]);
        }
    }
    ArrayList<String> rest = bag;
    if (rest.size()==0) {
        // TODO refine error message after preparing launch script.
        System.err.println("[ApgasMPILauncher] Error Main Class Required.");
        MPI.Finalize();
        System.exit(0);
    }
    if(mpiArgs.length<3) {
	mpiArgs = new String[] { "0", "0", "native" };
	if(verboseLauncher) {
	    System.err.println("[ApgasMPILauncher] use default parameters { \"0\", \"0\", \"native\" } to call MPI.Init()");
	}
    }
    MPI.Init(mpiArgs);	
    
    commRank = MPI.COMM_WORLD.Rank();
    commSize = MPI.COMM_WORLD.Size();

    if(verboseLauncher) 
      System.err.println("[ApgasMPILauncher] rank = " + commRank);

    System.setProperty(Configuration.APGAS_PLACES, Integer.toString(commSize));
    System.setProperty(Config.APGAS_LAUNCHER, "handist.collections.mpi.ApgasMPILauncher");

    if (commRank == 0) {
      TeamedPlaceGroup.worldSetup();
      try {
	final Method mainMethod = Class.forName(rest.remove(0)).getMethod("main",
            String[].class);
	String[] targetArgs = new String[rest.size()];
	rest.toArray(targetArgs);
	mainMethod.invoke(null, new Object[] { targetArgs });
      } catch (final Exception e) {
        e.printStackTrace();
      }
    } else {
      slave();
      if(verboseLauncher) 
	System.err.println("[ApgasMPILauncher] rank = " + commRank + ", here" + here());
      TeamedPlaceGroup.worldSetup();
    }
    TeamedPlaceGroup.readyToClose(commRank==0);
    MPI.Finalize();
    System.exit(0);
  }
}
