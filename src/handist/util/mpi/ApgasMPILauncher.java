package handist.util.mpi;

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
import mpi.MPI;
import mpi.MPIException;

/**
 * The {@link ApgasMPILauncher} class implements a launcher using MPI.
 */
final public class ApgasMPILauncher implements Launcher {

  static int commRank;
  static int commSize;

  /**
   * Constructs a new {@link ApgasMPILauncher} instance.
   */
  public ApgasMPILauncher() {
  }

  /**
   * Launches one process with the given command line at the specified host.
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
      System.err.println("[ApgasMPILauncher] " + Configuration.APGAS_PLACES
          + " should be equal to number of MPI processes " + commSize);
      MPI.Finalize();
      System.exit(-1);
    }

    final byte[] baCommand = serializeToByteArray(
        command.toArray(new String[command.size()]));
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
        System.err.println("[" + commRank + "] setProperty \"" + kv[0]
            + "\" = \"" + kv[1] + "\"");
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

  // public static void main(String[] args) throws Exception {
  /*
   * int[] testArray = stringToIntArray(args[1]); String testStr =
   * intArrayToString(testArray); System.out.println(testStr);
   */

  /*
   * MPI.Init(args); commRank = MPI.COMM_WORLD.getRank(); commSize =
   * MPI.COMM_WORLD.getSize();
   *
   * String msg = "cram date dirt dish"; int[] msglen = new int[1]; int[]
   * msgBuf; if (commRank == 0) { msgBuf = stringToIntArray(msg); msglen[0] =
   * msgBuf.length; MPI.COMM_WORLD.bcast(msglen, 1, MPI.INT, 0);
   * MPI.COMM_WORLD.bcast(msgBuf, msglen[0], MPI.INT, 0); } else {
   * MPI.COMM_WORLD.bcast(msglen, 1, MPI.INT, 0); msgBuf = new int[msglen[0]];
   * MPI.COMM_WORLD.bcast(msgBuf, msglen[0], MPI.INT, 0); }
   *
   * for (int i = 0; i < commSize; i++) { MPI.COMM_WORLD.barrier(); if (i ==
   * commRank) { System.out.println("[" + commRank + "] \"" +
   * intArrayToString(msgBuf) + "\""); } }
   *
   * MPI.Finalize();
   */

  /*
   * MPI.Init(args); commRank = MPI.COMM_WORLD.getRank(); commSize =
   * MPI.COMM_WORLD.getSize();
   *
   * String[] srcstr = new String[]{"bike", "cake", "camp", "chip", "dash",
   * "mesh", "mouth", "sky"}; if (commRank == 0) { byte[] barray =
   * serializeToByteArray(srcstr); String[] dststr = (String[])
   * deserializeFromByteArray(barray); for (int i = 0; i < dststr.length; i++) {
   * System.out.println(srcstr[i] + " -> " + dststr[i]); } } else { }
   *
   * MPI.Finalize();
   */
  // }

  public static void main(String[] args) throws Exception {
	ArrayList<String> bag = new ArrayList<>();
	String[] mpiArgs = new String[0];
	String[] restArgs = new String[0];
	for(int i = 0; i<args.length; i++) {
		if(args[i].equals("-body")) {
			mpiArgs = bag.toArray(mpiArgs);
			bag.clear();
		} else {
			bag.add(args[i]);
		}
	}
	restArgs = bag.toArray(restArgs);
	System.err.println("mpiArgs:"+ java.util.Arrays.toString(mpiArgs));
	System.err.println("restArgs:"+java.util.Arrays.toString(restArgs));

    if (mpiArgs.length==0 || restArgs.length==0) {
        // TODO refine error message after preparing launch script.
        System.err.println("[ApgasMPILauncher] Error Main Class Required.");
        MPI.Finalize();
        System.exit(0);
      }

    MPI.Init(mpiArgs);
    commRank = MPI.COMM_WORLD.Rank();
    commSize = MPI.COMM_WORLD.Size();

    System.err.println("[ApgasMPILauncher] rank = " + commRank);

    System.setProperty(Configuration.APGAS_PLACES, Integer.toString(commSize));
    System.setProperty(Config.APGAS_LAUNCHER, "handist.util.mpi.ApgasMPILauncher");

    if (commRank == 0) {

      try {
        final Method mainMethod = Class.forName(restArgs[0]).getMethod("main",
            String[].class);
        final Object[] mainArgs = new Object[1];
        mainArgs[0] = restArgs;
        mainMethod.invoke(null, mainArgs);
        MPI.Finalize();
        System.exit(0);
      } catch (final Exception e) {
        e.printStackTrace();
        MPI.Finalize();
        System.exit(0);
      }
    } else {
      slave();
    }

    MPI.Finalize();
    System.exit(0);
  }

}
