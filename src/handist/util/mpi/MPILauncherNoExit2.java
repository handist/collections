package handist.util.mpi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.Permission;
import java.util.List;

import apgas.Configuration;
import apgas.GlobalRuntime;
import apgas.impl.Config;
import apgas.impl.Launcher;
import mpi.MPI;
import mpi.MPIException;

/**
 * The {@link MPILauncherNoExit2} class implements a launcher for the apgas runtime
 * using MPI. Programs should be run in the following way:
 * <p>
 * <em>mpirun -np <nb hosts> -host <, seperated list of hosts> java -cp <:
 * seperated list of jar> apgas.mpi.MPILauncher <class with main to be launched
 * on place 0> <arguments for said class></em>
 * <p>
 * For instance, to launch the HelloWorld example with message "Hello" on 4
 * servers, a solution would be:
 * <p>
 * <em>mpirun -np 4 -host piccolo04,piccolo05,piccolo06,piccolo07 java -cp
 * apgas.jar:apgasExamples.jar:kryo.jar:mpi.jar:reflectasm.jar:hazelcast.jar:minlog.jar:objenesis.jar
 * apgas.mpi.MPILauncher apgas.examples.HelloWorld Hello</em>
 *
 * @author Toshiyuki
 */
final public class MPILauncherNoExit2 implements Launcher {

	/**
	 * Exception thrown when the application given as parameter calls 
	 * {@link System#exit(int)} rather than exiting the whole application.
	 *
	 */
	protected static class ExitException extends SecurityException 
    {
        /** Serial Version UID */
		private static final long serialVersionUID = -5788271742636040872L;
		public final int status;
        public ExitException(int status) 
        {
            super("The MPI launcher prevented the call to System.exit(" + status + ")");
            this.status = status;
        }
    }

	/**
	 * Security manager that prevents any call to {@link System#exit(int)}.
	 * @author Patrick
	 *
	 */
    private static class NoExitSecurityManager extends SecurityManager 
    {
        @Override
        public void checkPermission(Permission perm) 
        {
            // allow anything.
        }
        @Override
        public void checkPermission(Permission perm, Object context) 
        {
            // allow anything.
        }
        @Override
        public void checkExit(int status) 
        {
            super.checkExit(status);
            throw new ExitException(status);
        }
    }
	
  /** Identifier of this place */
  static int commRank;
  /** Number of places in the system */
  static int commSize;

  static boolean finalizeCalled = false;

  /**
   * Set in the main method according to the value set by
   * {@link Configuration#APGAS_VERBOSE_LAUNCHER}
   */
  static boolean verboseLauncher;

  /**
   * Constructs a new {@link MPILauncherNoExit2} instance.
   */
  public MPILauncherNoExit2() {
  }

  /**
   * Launches one process with the given command line at the specified host.
   * <p>
   * Should not be called in practice as every java processes are supposed to be
   * launched using the `mpirun` command. Implementation will print an "Internal
   * error" on the {@link System#err} before calling for the program's
   * termination and exiting with return code -1.
   *
   * @param command
   *          command line
   * @param host
   *          host
   * @param verbose
   *          dumps the executed commands to stderr
   * @return the process object
   * @throws Exception
   *           if launching fails
   */
  @Override
  public Process launch(List<String> command, String host, boolean verbose)
      throws Exception {

    System.err.println("[MPILauncher] Internal Error");
    MPI.Finalize();
    System.exit(-1);

    return null;
  }

  /**
   * Launches n processes with the given command line and host list. The first
   * host of the list is skipped. If the list is incomplete, the last host is
   * repeated.
   *
   * @param n
   *          number of processes to launch
   * @param command
   *          command line
   * @param hosts
   *          host list (not null, not empty, but possibly incomplete)
   * @param verbose
   *          dumps the executed commands to stderr
   * @throws Exception
   *           if launching fails
   */
  @Override
  public void launch(int n, List<String> command, List<String> hosts,
      boolean verbose) throws Exception {

    if (n + 1 != commSize) {
      System.err.println("[MPILauncher] " + Configuration.APGAS_PLACES
          + " should be equal to number of MPI processes " + commSize);
      MPI.Finalize();
      System.exit(-1);
    }

    final byte[] baCommand = serializeToByteArray(
        command.toArray(new String[command.size()]));
    final int[] msglen = new int[1];
    msglen[0] = baCommand.length;
    MPI.COMM_WORLD.Bcast(msglen, 0, 1, MPI.INT, 0);
    MPI.COMM_WORLD.Bcast(baCommand, 0, msglen[0], MPI.BYTE, 0);
  }

  /**
   * Sets up the apgas runtime for it to wait for activities to be spawned on
   * this place.
   *
   * @throws Exception
   *           if some problem occurs with the MPI runtime
   */
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
        if (verboseLauncher) {
          System.err.println("[" + commRank + "] setProperty \"" + kv[0]
              + "\" = \"" + kv[1] + "\"");
        }

        System.setProperty(kv[0], kv[1]);
      }
    }

    GlobalRuntime.getRuntime();
  }

  /**
   * Shuts down the local process launched using MPI.
   */
  @Override
  public void shutdown() {
    if (verboseLauncher) {
      System.err.println("[MPILauncherNoExit2] About to call MPI.Finalize on rank" + commRank );
    }
    try {
      if (!finalizeCalled) {
         finalizeCalled = true;
         MPI.Finalize();
      }
    } catch (final MPIException e) {
      System.err.println("[MPILauncher] Error on Shutdown at rank " + commRank);
      e.printStackTrace();
    }
    System.exit(0);
  }

  /**
   * Checks that all the processes launched are healthy.
   * <p>
   * Current implementation of {@link MPILauncherNoExit2} always returns true.
   *
   * @return true if all subprocesses are healthy
   */
  @Override
  public boolean healthy() {
    // TODO
    return true;
  }

  /**
   * Converts a String to an int array
   *
   * @param src
   *          String to be converted
   * @return array of integer corresponding to the given parameter
   * @see #intArrayToString(int[])
   */
  static int[] stringToIntArray(String src) {
    final char[] charArray = src.toCharArray();
    final int[] intArray = new int[charArray.length];
    for (int i = 0; i < charArray.length; i++) {
      intArray[i] = charArray[i];
    }
    return intArray;
  }

  /**
   * Converts an int array back into the String it represents
   *
   * @param src
   *          the integer array to be converted back into a String
   * @return the constructed String
   * @see #stringToIntArray(String)
   */
  static String intArrayToString(int[] src) {
    final char[] charArray = new char[src.length];
    for (int i = 0; i < src.length; i++) {
      charArray[i] = (char) src[i];
    }
    final String str = new String(charArray);
    return str;
  }

  /**
   * Serializer method
   *
   * @param obj
   *          Object to be serialized
   * @return array of bytes
   * @throws IOException
   *           if an I/O exception occurs
   * @see #deserializeFromByteArray(byte[])
   */
  static byte[] serializeToByteArray(Serializable obj) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final ObjectOutputStream oos = new ObjectOutputStream(baos);
    oos.writeObject(obj);
    return baos.toByteArray();
  }

  /**
   * Deserializer method
   *
   * @param barray
   *          byte array input
   * @return Object constructed from the input
   * @throws IOException
   *           if an I/O occurs
   * @throws ClassNotFoundException
   *           if the class could not be identified
   * @see #serializeToByteArray(Serializable)
   */
  static Object deserializeFromByteArray(byte[] barray)
      throws IOException, ClassNotFoundException {
    final ByteArrayInputStream bais = new ByteArrayInputStream(barray);
    final ObjectInputStream ois = new ObjectInputStream(bais);
    return ois.readObject();
  }

  /**
   * Main method of the MPILauncher class
   * <p>
   * Sets up the APGAS environment using an mpi launcher. Rank 0 of the
   * processes will launch the main method of the class specified as parameter
   * with the arguments specified afterward. Other processes will only setup
   * their apgas environment and wait for incoming activities.
   * <p>
   * This main method takes at least one argument, the fully qualified name of
   * the class whose main method is to be run. Arguments for that class' main
   * need to to follow that first argument.
   *
   * @param args
   *          Path to the class whose main method is to be run, followed by the
   *          arguments of that classe's main method.
   * @throws Exception
   *           if MPI exception occur
   */
  public static void main(String[] args) throws Exception {
    int exitStatus = 0;
	MPI.Init(args);
    commRank = MPI.COMM_WORLD.Rank();
    commSize = MPI.COMM_WORLD.Size();

    verboseLauncher = Boolean.parseBoolean(
        System.getProperty(Configuration.APGAS_VERBOSE_LAUNCHER, "false"));

    if (verboseLauncher) {
      System.err.println("[MPILauncher] rank = " + commRank);
    }

    if (args.length < 1) {
      System.err.println("[MPILauncher] Error Main Class Required");
      MPI.Finalize();
      System.exit(0);
    }

    /*
     * Extracts the arguments destined for the main method of the specified
     * class.
     */
    final String[] newArgs = new String[args.length - 1];
    System.arraycopy(args, 1, newArgs, 0, args.length - 1);

    // Sets the number of places according to the arguments given to `mpirun`
    // command
    System.setProperty(Configuration.APGAS_PLACES, Integer.toString(commSize));
    // Sets the launcher to be of MPILauncher class. This will make the apgas
    // runtime use the MPILauncher shutdown method when the apgas shutdown
    // method is launched
    System.setProperty(Config.APGAS_LAUNCHER, "apgas.mpi.MPILauncherNoExit2");

    /*
     * If this place is the "master", i.e. rank, launches the main method of the
     * class specified as parameter. If it is not the "master", sets up the
     * APGAS runtime and waits till asynchronous tasks are submitted to this
     * place.
     */
    if (commRank == 0) {
      try {
        GlobalRuntime.getRuntime();
        final Method mainMethod = Class.forName(args[0]).getMethod("main",
            String[].class);
        final Object[] mainArgs = new Object[1];
        mainArgs[0] = newArgs;
        
        // Prevent call to exit()
        System.setSecurityManager(new NoExitSecurityManager());
        mainMethod.invoke(null, mainArgs);
      } catch (final ClassNotFoundException e) {
        System.err.println(
            "[MPILauncher] Error: Class " + args[0] + " could not be found");
      } catch (final NoSuchMethodException e) {
        System.err.println("[MPILauncher] Error: Class " + args[0]
            + " does not have a main method");
      } catch (final InvocationTargetException e) {
    	  if (e.getCause() instanceof ExitException) {
            // Obtain the exit status to make host 0 transmit it
            // after the program shutdown occurs
            exitStatus = ((ExitException)e.getCause()).status;
            if (verboseLauncher) {
              System.err.println("[MPILauncherNoExit] exit code " + exitStatus + " was intercepted");
            }
    	  }
      } catch (final Exception e) {
        e.printStackTrace();
      } finally {
    	  // Remove the security manager to allow shutdown
    	  System.setSecurityManager(null);
      }
      
      //Initiating shutdown
      if (verboseLauncher) {
      	System.err.println("[MPILauncher] MPI rank" + commRank + " is initiating shutdown");
      }
//      for (Place p : apgas.Constructs.places()) {
//    	  if (p.id != 0) {
//    		((GlobalRuntimeImpl) GlobalRuntime.getRuntime()).immediateAsyncAt(p, () -> {
//                  if (verboseLauncher) {System.err.println("[MPILauncherNoExit2] " + apgas.Constructs.here() + " has received the task making it shut down");}
//                  GlobalRuntime.getRuntime().shutdown();  
//                 System.exit(0); // Should activate the shutdown hook of the remote host.
//                });
//   	  }
//      }
//      // Call to finalize for host 0 as well
//      try {
//          MPI.Finalize();
//      } catch (final MPIException e) {
//        System.err.println("[MPILauncher] Error on Shutdown at rank " + commRank);
//        e.printStackTrace();
//      } finally {
//        System.exit(exitStatus);
//      }
//      
    } else {
      slave();
    }
    try {
      if (!finalizeCalled) {
        finalizeCalled = true;
        MPI.Finalize();
      }
      System.exit(0);
    } catch (final MPIException e) {
      System.err.println(
          "[MPILauncher] Error on Finalize - main method at rank " + commRank);
      e.printStackTrace();
    }
  }
}
