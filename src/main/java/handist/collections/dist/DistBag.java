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

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import apgas.Constructs;
import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.Bag;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.function.DeSerializer;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.Serializer;
import mpi.MPI;
import mpi.MPIException;

/**
 * A class for handling objects at multiple places.
 * It is allowed to add new elements dynamically.
 * This class provides methods for load balancing.
 * <p>
 * Note: In its current implementation, there are some limitations.
 * <ul>
 * 	<li>There is only one load balancing method
 *  <li>The method flattens the number of elements of the all places
 * </ul>
 * 
 * @param <T> type of the elements handled by the {@link DistBag}.
 */
public class DistBag<T> extends Bag<T> implements AbstractDistCollection<DistBag<T>>, SerializableWithReplace {
	/* implements Container[T], ReceiverHolder[T] */
	
	public class DistBagGlobal extends GlobalOperations<DistBag<T>> {
		DistBagGlobal(DistBag<T> handle) {
			super(handle);
		}
	}
	
	public class DistBagTeam extends TeamOperations<DistBag<T>> {
		
		DistBagTeam(DistBag<T> handle) {
			super(handle);
		}

		@Override
		public void updateDist() {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void size(long[] result) {
			TeamedPlaceGroup pg = handle.placeGroup();
			long localSize = handle.size(); // cast from int to long here
			long[] sendbuf = new long[]{ localSize };
			// team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
			try {
				pg.comm.Allgather(sendbuf, 0, 1, MPI.LONG, result, 0, 1, MPI.LONG);
			} catch (MPIException e) {
				e.printStackTrace();
				throw new Error("[DistMap] network error in team().size()");
			}
		}
		
	}

	private static int _debug_level = 5;

	final GlobalID id;
	public transient float[] locality;
	public final TeamedPlaceGroup placeGroup;

	/** Handle to Global operations on the DistBag instance */
	public DistBag<T>.DistBagGlobal GLOBAL;
	/** Handle to TEAM operations on the DistBag instance */
	public DistBag<T>.DistBagTeam TEAM;

	/**
	 * Create a new DistBag.
	 * Place.places() is used as the PlaceGroup.
	 */
	public DistBag() {
		this(TeamedPlaceGroup.getWorld());
	}

	/**
	 * Create a new DistBag using the given arguments.
	 *
	 * @param pg the places susceptible to interact with this instance.
	 */
	public DistBag(TeamedPlaceGroup pg) {
		this(pg, new GlobalID());
	}

	protected DistBag(TeamedPlaceGroup pg, GlobalID globalId) {
		super();
		id = globalId;
		placeGroup = pg;
		locality = new float[pg.size];
		Arrays.fill(locality, 1.0f);
		id.putHere(this);
		GLOBAL = new DistBagGlobal(this);
		TEAM = new DistBagTeam(this);
	}

	//  Method was moved to TEAM and GLOBAL handles
//	@Override
//	public void distSize(long[] result) {
//		TeamedPlaceGroup pg = this.placeGroup;
//		long localSize = size(); // int->long
//		long[] sendbuf = new long[]{ localSize };
//		// team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
//		try {
//			pg.comm.Allgather(sendbuf, 0, 1, MPI.LONG, result, 0, 1, MPI.LONG);
//		} catch (MPIException e) {
//			e.printStackTrace();
//			throw new Error("[DistMap] network error in checkDistInfo()");
//		}
//	}

	/**
	 * gather all place-local elements to the root Place.
	 *
	 * @param root the place where the result of reduction is stored.
	 */
	@SuppressWarnings("unchecked")
	public void gather(Place root) {
		Serializer serProcess = (ObjectOutputStream ser) -> {
			ser.writeObject(new Bag(this));
		};
		DeSerializerUsingPlace desProcess = (ObjectInputStream des, Place place) -> {
			Bag<T> imported = (Bag<T>) des.readObject();
			addBag(imported);
		};
		CollectiveRelocator.gatherSer(placeGroup, root, serProcess, desProcess);
		if (!here().equals(root)) {
			clear();
		}
	}

	public void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManagerLocal mm) throws Exception {
		for (IntLongPair pair : moveList) {
			if (_debug_level > 5) {
				System.out.println("MOVE src: " + here() + " dest: " + pair.first + " size: " + pair.second);
			}
			if (pair.second > Integer.MAX_VALUE)
				throw new Error("One place cannot receive so much elements: " + pair.second);
			moveAtSyncCount((int) pair.second, placeGroup.get(pair.first), mm);
		}
	}
	/*
    public def versioning(srcName : String){
        return new BranchingManager[DistBag[T], List[T]](srcName, this);
    }
	 */


	/**
	 * Return a Container that has the same values of DistBag's local storage.
	 *
	 * @return a Container that has the same values of local storage.
	 */
	/*
    public Collection<T> clone(): Container[T] {
        return data.clone();
    }*/

	/**
	 * Removes the specified number of entries from the local Bag and prepares
	 * them to be transfered to the specified place when the 
	 * {@link MoveManagerLocal#sync()} method of the {@link MoveManagerLocal} is
	 * called.
	 * <p>
	 * The objects are not removed from the local collection until method 
	 * {@link MoveManagerLocal#sync()} is called.
	 * If the {@code destination} is the local placce, this method has no 
	 * effects.
	 * 
	 * @param count number of objects to move from this instance
	 * @param destination the destination of the objects
	 * @param mm move manager in charge of making the transfer
	 */
	@SuppressWarnings("unchecked")
	public void moveAtSyncCount(final int count, Place destination, MoveManagerLocal mm) {
		if (destination.equals(Constructs.here()))
			return;
		final DistBag<T> collection = this;
		Serializer serialize = (ObjectOutputStream s) -> {
			s.writeObject(this.remove(count));
		};
		DeSerializer deserialize = (ObjectInputStream ds) -> {
			Collection<T> imported = (Collection<T>) ds.readObject();
			collection.addAll(imported);
		};
		mm.request(destination, serialize, deserialize);
	}


	/*
    public def integrate(src : List[T]) {
        // addAll(src);
        throw new UnsupportedOperationException();
    }*/

	// TODO ...
	public void setupBranches(SerializableBiConsumer<Place,DistBag<T>> gen) {
		final DistBag<T> handle = this;
		finish(()->{
			placeGroup.broadcastFlat(()->{
				gen.accept(here(), handle);
			});
		});
	}

	public Object writeReplace() throws ObjectStreamException {
		final TeamedPlaceGroup pg1 = placeGroup;
		final GlobalID id1 = id;
		return new LazyObjectReference<DistBag<T>>(pg1, id1, ()-> {
			return new DistBag<T>(pg1, id1);
		});
	}

	@Override
	public float[] locality() {
		return locality;
	}

	@Override
	public GlobalID id() {
		return id;
	}

	@Override
	public TeamOperations<DistBag<T>> team() {
		return TEAM;
	}

	@Override
	public GlobalOperations<DistBag<T>> global() {
		return GLOBAL;
	}

	@Override
	public TeamedPlaceGroup placeGroup() {
		return placeGroup;
	}

}
