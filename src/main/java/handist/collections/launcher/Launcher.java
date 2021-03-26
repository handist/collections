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
        TeamedPlaceGroup.setup();
        MPILauncher.main(args);
    }

}
