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

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Lifeline network describing a directed loop through all the places considered
 * in the given TeamedPlaceGroup
 *
 * @author Patrick Finnerty
 *
 */
public final class Loop extends Lifeline {

    /**
     * Constructor
     *
     * @param pg place group under consideration for this lifeline network.
     */
    public Loop(TeamedPlaceGroup pg) {
        super(pg);
    }

    @Override
    public List<Place> lifeline(Place p) {
        final int index = sortedListOfPlaces.indexOf(p);
        assertTrue("Place " + p + " was not within the list of places for this lifeline network " + sortedListOfPlaces,
                index >= 0); // "p" has to be within the sortedListOfPlaces
        final int lifeline = index + 1 < sortedListOfPlaces.size() ? index + 1 : 0;

        final List<Place> lifelineList = new ArrayList<>();
        lifelineList.add(sortedListOfPlaces.get(lifeline));
        return lifelineList;
    }

    @Override
    public List<Place> reverseLifeline(Place p) {
        final int index = sortedListOfPlaces.indexOf(p);
        assertTrue("Place " + p + " was not within the list of places for this lifeline network " + sortedListOfPlaces,
                index >= 0); // "p" has to be within the sortedListOfPlaces
        final int lifeline = index - 1 >= 0 ? index - 1 : sortedListOfPlaces.size() - 1;

        final List<Place> lifelineList = new ArrayList<>();
        lifelineList.add(sortedListOfPlaces.get(lifeline));
        return lifelineList;
    }

}
