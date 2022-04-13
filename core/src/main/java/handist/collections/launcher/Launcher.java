/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections.launcher;

import apgas.mpi.MPILauncher;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Launcher for programs that use the distributed collections library. This main
 * method is only used to add the plugin needed by the library before directly
 * calling the {@link MPILauncher} with the arguments provided.
 */
public class Launcher {

    /**
     * Sets up the plugin needed by the distributed collections library before
     * delegating the application's launch to the {@link MPILauncher} class
     *
     * @param args program arguments
     * @throws Exception if such an exception is thrown by the {@link MPILauncher}
     */
    public static void main(String[] args) throws Exception {
        String[] newArgs = args;

        // Insert the MPJ "0 0 native" arguments if necessary
        // First, establish if we are running with MPJ
        boolean isMPJ = false;
        try {
            final Class<?> mpjdevCommClass = Class.forName("mpjdev.Comm");
            isMPJ = (mpjdevCommClass != null);
        } catch (final Exception e) {
            // Ignore any exception
        }
        // Insert the parameters if running with MPJ
        if (isMPJ) {
            newArgs = new String[args.length + 3];
            newArgs[0] = "0";
            newArgs[1] = "0";
            newArgs[2] = "native";
            for (int i = 0; i < args.length; i++) {
                newArgs[i + 3] = args[i];
            }
        }

        // Register the necessary plugin for the APGAS-MPI launcher
        TeamedPlaceGroup.setup();

        // Launch the APGAS-MPI launcher
        MPILauncher.main(newArgs);
    }

}
