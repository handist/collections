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
package handist.collections.glb.lifeline;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Lifeline implementation which provide no lifeline between places. Using this
 * lifeline implementation effectively disables any inter-host load balancing
 * mechanism.
 *
 * @author Patrick Finnerty
 *
 */
public class NoLifeline extends Lifeline {

    /** Constructor */
    public NoLifeline(TeamedPlaceGroup pg) {
        super(pg);
    }

    /**
     * Returns an empty array (no lifelines to establish)
     */
    @Override
    public List<Place> lifeline(Place p) {
        return new ArrayList<>(0);
    }

    /**
     * Returns an empty array (there are no places which can establish a lifeline on
     * this place
     */
    @Override
    public List<Place> reverseLifeline(Place p) {
        return new ArrayList<>(0);
    }

}
