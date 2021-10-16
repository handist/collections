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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Hypercube lifeline strategy. Under this lifeline strategy, all hosts are
 * numbered from 0 to n-1. Hosts share a lifeline with the hosts that with the
 * neighbors that have at most one digit difference in their rank written in
 * binary.
 * <p>
 * Using 4 hosts, this lifeline strategy describes a square between the hosts.
 * Using 8 hosts, a 3D cube is drawn between the hosts.
 *
 * @author Patrick Finnerty
 *
 */
public class Hypercube extends Lifeline implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = 7684362314472294594L;

    /**
     * Constructor
     *
     * @param pg the group of places under consideration
     */
    public Hypercube(TeamedPlaceGroup pg) {
        super(pg);
    }

    @Override
    public List<Place> lifeline(Place p) {
        final int index = sortedListOfPlaces.indexOf(p);
        final ArrayList<Place> lifeline = new ArrayList<>();

        for (int mask = 1;; mask *= 2) { // Loop exited with a break
            final int lifelineIndex = index ^ mask;
            if (lifelineIndex < sortedListOfPlaces.size()) {
                lifeline.add(sortedListOfPlaces.get(lifelineIndex));
            } else {
                break;
            }
        }
        return lifeline;
    }

    /*
     * In the case of the hypercube lifeline, if A has a lifeline of B, then B has a
     * lifeline on A. As it is a reciprocal relationship, we do not not need to
     * reimplement this method and instead rely on the existing {@link
     * #lifeline(Place)} method.
     */
    @Override
    public List<Place> reverseLifeline(Place p) {
        return lifeline(p);
    }
}
