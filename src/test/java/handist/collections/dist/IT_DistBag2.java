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
package handist.collections.dist;

import java.io.Serializable;

import org.junit.Ignore;
import org.junit.runner.RunWith;

import handist.collections.dist.DistBag.DistBagTeam;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * Test class dedicated to class {@link DistBag} and its Global, and
 * {@link DistBagTeam}
 */
@Ignore
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistBag2 implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 8208565684336617060L;

    // TODO implement tests for the various features of class DistBag
}
