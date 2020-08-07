/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static apgas.Constructs.*;

import java.util.SortedMap;

import apgas.Place;
import apgas.SerializableJob;
import apgas.util.GlobalID;
import mpi.Datatype;
import mpi.MPIException;


/**
 * SinglePlaceGroup is a place group consisting of a single place.
 * 
 *  It simply skips collective communications.
 *
 */
public class SinglePlaceGroup extends TeamedPlaceGroup {

	static SinglePlaceGroup world = new SinglePlaceGroup();
	public static SinglePlaceGroup getWorld() {
		return world;
	}

	protected SinglePlaceGroup() {
	    super();
	}
	@Override   
	public void Alltoallv(Object byteArray, int soffset, int[] sendSize, int[] sendOffset, Datatype stype,
			Object recvbuf, int roffset, int[] rcvSize, int[] rcvOffset, Datatype rtype) throws MPIException {
        /* do nothing */
	}
	@Override   
	public void barrier() {
        /* do nothing */	    
	}
	@Override   
	public void broadcastFlat(SerializableJob run) {
		// TODO
		finish(() -> {
			run.run();
		});
	}
	@Override   
	public Place get(int rank) {
		return here();
	}
	@Override  
	public int rank(Place place) {
		if(place.equals(here())) return 0;
		throw new RuntimeException("[TeamedPlaceGroup] " + place + " is not a member of " + this + ".");
	}

	// TODO
	// split, relocate feature
	@Override   
	public void remove(GlobalID id) {
		// TODO
	}
    @Override
	public TeamedPlaceGroup split(SortedMap<Integer, Integer> rank2color) {
        /* do nothing */
        return this;
	}
    @Override
	public TeamedPlaceGroup splitHalf() {
        /* do nothing */
        return this;
	}
    @Override
	public String toString() {
		return "SinglePlaceGroup";
	}

}

