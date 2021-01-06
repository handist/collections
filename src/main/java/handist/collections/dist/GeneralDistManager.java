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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import apgas.util.GlobalID;
import handist.collections.dist.util.IntFloatPair;
import handist.collections.dist.util.IntLongPair;

public abstract class GeneralDistManager<T> implements Serializable {

	private static int _debug_level = 5;


	/** Serial Version UID */
	private static final long serialVersionUID = 7184736551394411890L;
	protected T branch;

	final GlobalID id;
	// @TransientInitExpr(getLocalData())

	transient float[] locality;
	/*
	 * Ensure calling updateDist() before balance() balance() should be called in
	 * all places
	 */
    public final TeamedPlaceGroup placeGroup; // may be packed into T? or globalID??

    /*
	public GeneralDistManager(TeamedPlaceGroup pg, T branch) {
		this(pg, new GlobalID(), branch);
	}*/
	
	public GeneralDistManager(TeamedPlaceGroup pg, GlobalID id, T branch) {
		this.id = id;
		this.placeGroup = pg;
		this.branch = branch;
		this.locality = new float[pg.size];
		Arrays.fill(locality, 1.0f);
		id.putHere(branch);
	}

	public void balance() {
		final TeamedPlaceGroup pg = placeGroup;
		final GeneralDistManager<T> handle = this;
		pg.broadcastFlat(() -> {
			handle.teamedBalance();
		});
	};

	public void balance(final float[] balance) {
		balanceSpecCheck(balance);
		TeamedPlaceGroup pg = this.placeGroup;
		GeneralDistManager<T> handle = this;
		pg.broadcastFlat(() -> {
			handle.teamedBalance(balance);
		});
	}

	protected void balanceSpecCheck(final float[] balance) {
		if (balance.length != placeGroup.size) {
			throw new RuntimeException("[AbstractDistCollection");
		}        
	}

	abstract public void checkDistInfo(long[] result);

	/**
	 * Destroy an instance of AbstractDistCollection.
	 */
	public void destroy() {
		placeGroup.remove(id);
	}

	abstract protected void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManagerLocal mm)
			throws Exception;

	/**
	 * Return the PlaceGroup.
	 *
	 * @return PlaceGroup.
	 */
	public TeamedPlaceGroup placeGroup() {
		return placeGroup;
	}

	// TODO
	// public abstract void integrate(T src);
	public void teamedBalance() {
		teamedBalance(new MoveManagerLocal(placeGroup));
	}

	public void teamedBalance(final float[] balance) {
		teamedBalance(balance, new MoveManagerLocal(placeGroup));
	}

	public void teamedBalance(final float[] newLocality, final MoveManagerLocal mm) {
		// Rail.copy[Float](ne wL ocality, locality)

		if (newLocality.length != placeGroup.size())
			throw new RuntimeException("[DistCol] the size of newLocality must be the same with placeGroup.size()");
		System.arraycopy(newLocality, 0, locality, 0, locality.length);
		teamedBalance(mm);
	}

	// TODO
	// maybe these methods should move to the interface like RelocatableCollection or RelocatableMap
	// as default methods.
	public void teamedBalance(MoveManagerLocal mm) {
		final int pgSize = placeGroup.size();
		final IntFloatPair[] listPlaceLocality = new IntFloatPair[pgSize];
		float localitySum = 0.0f;
		long globalDataSize = 0;
		final long[] localDataSize = new long[pgSize];

		for (int i = 0; i < pgSize; i++) {
			localitySum += locality[i];
		}
		checkDistInfo(localDataSize);

		for (int i = 0; i < pgSize; i++) {
			globalDataSize += localDataSize[i];
			final float normalizeLocality = locality[i] / localitySum;
			listPlaceLocality[i] = new IntFloatPair(i, normalizeLocality);
		}
		Arrays.sort(listPlaceLocality, (IntFloatPair a1, IntFloatPair a2) -> {
			return Float.compare(a1.second, a2.second);
		});

		if (_debug_level > 5) {
			for (IntFloatPair pair : listPlaceLocality) {
				System.out.print("(" + pair.first + ", " + pair.second + ") ");
			}
			System.out.println();
			placeGroup.barrier(); // for debug print
		}

		IntFloatPair[] cumulativeLocality = new IntFloatPair[pgSize];
		float sumLocality = 0.0f;
		for (int i = 0; i < pgSize; i++) {
			sumLocality += listPlaceLocality[i].second;
			cumulativeLocality[i] = new IntFloatPair(listPlaceLocality[i].first, sumLocality);
		}
		cumulativeLocality[pgSize - 1] = new IntFloatPair(listPlaceLocality[pgSize - 1].first, 1.0f);

		if (_debug_level > 5) {
			for (int i = 0; i < pgSize; i++) {
				IntFloatPair pair = cumulativeLocality[i];
				System.out.print("(" + pair.first + ", " + pair.second + ", " + localDataSize[pair.first] + "/"
						+ globalDataSize + ") ");
			}
			System.out.println();
			placeGroup.barrier(); // for debug print
		}

		final ArrayList<ArrayList<IntLongPair>> moveList = new ArrayList<>(pgSize); // ArrayList(index of dest Place, num
		// data to export)
		ArrayList<IntLongPair> stagedData = new ArrayList<>(); // ArrayList(index of src, num data to export)
		long previousCumuNumData = 0;

		for (int i = 0; i < pgSize; i++) {
			moveList.add(new ArrayList<IntLongPair>());
		}

		for (int i = 0; i < pgSize; i++) {
			int placeIdx = cumulativeLocality[i].first;
			float placeLocality = cumulativeLocality[i].second;
			long cumuNumData = (long) (((float) globalDataSize) * placeLocality);
			long targetNumData = cumuNumData - previousCumuNumData;
			if (localDataSize[placeIdx] > targetNumData) {
				stagedData.add(new IntLongPair(placeIdx, localDataSize[placeIdx] - targetNumData));
				if (_debug_level > 5) {
					System.out.print(
							"stage src: " + placeIdx + " num: " + (localDataSize[placeIdx] - targetNumData) + ", ");
				}
			}
			previousCumuNumData = cumuNumData;
		}
		if (_debug_level > 5) {
			System.out.println();
			placeGroup.barrier(); // for debug print
		}

		previousCumuNumData = 0;
		for (int i = 0; i < pgSize; i++) {
			int placeIdx = cumulativeLocality[i].first;
			float placeLocality = cumulativeLocality[i].second;
			long cumuNumData = (long) (((float) globalDataSize) * placeLocality);
			long targetNumData = cumuNumData - previousCumuNumData;
			if (targetNumData > localDataSize[placeIdx]) {
				long numToImport = targetNumData - localDataSize[placeIdx];
				while (numToImport > 0) {
					IntLongPair pair = stagedData.remove(0);
					if (pair.second > numToImport) {
						moveList.get(pair.first).add(new IntLongPair(placeIdx, numToImport));
						stagedData.add(new IntLongPair(pair.first, pair.second - numToImport));
						numToImport = 0;
					} else {
						moveList.get(pair.first).add(new IntLongPair(placeIdx, pair.second));
						numToImport -= pair.second;
					}
				}
			}
			previousCumuNumData = cumuNumData;
		}

		if (_debug_level > 5) {
			for (int i = 0; i < pgSize; i++) {
				for (IntLongPair pair : moveList.get(i)) {
					System.out.print("src: " + i + " dest: " + pair.first + " size: " + pair.second + ", ");
				}
			}
			System.out.println();
			placeGroup.barrier(); // for debug print
		}

		if (_debug_level > 5) {
			long[] diffNumData = new long[pgSize];
			for (int i = 0; i < pgSize; i++) {
				for (IntLongPair pair : moveList.get(i)) {
					diffNumData[i] -= pair.second;
					diffNumData[pair.first] += pair.second;
				}
			}
			for (IntFloatPair pair : listPlaceLocality) {
				System.out.print("(" + pair.first + ", " + pair.second + ", "

                        + (localDataSize[pair.first] + diffNumData[pair.first]) + "/" + globalDataSize + ") ");
			}
			System.out.println();
		}

		try {
			moveAtSyncCount(moveList.get(placeGroup.myrank), mm);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Error("[AbstractDistCollection] data transfer error raised.");
		}
	}

	// abstract public Object writeReplace() throws ObjectStreamException;
	// return new LaObjectReference(id, ()->{ new AbstractDistCollection<>());

	/*
    public final def printAllData(){
        for(p in placeGroup){
            at(p){
                printLocalData();
            }
        }
    }*/

}
