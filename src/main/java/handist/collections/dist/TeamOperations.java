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

import java.util.ArrayList;
import java.util.Arrays;

import apgas.Place;
import handist.collections.dist.util.IntFloatPair;
import handist.collections.dist.util.IntLongPair;

/**
 * Interface which defines the "team" operations that distributed collections
 * provide.
 * @param <T> type of the objects contained in the distributed collection
 * @param <C> type of the local handle on which the TeamOperations operate
 */
public abstract class TeamOperations<T, C extends AbstractDistCollection<T, C>> {
	
	static int _debug_level = 5;
	
	protected final C handle;
	
	/**
	 * Updates the distribution information of all local handles. 
	 */
	public abstract void updateDist();
	
	/**
	 * Computes and gathers the size of each local collection into the provided
	 * array. This operation usually requires that all the hosts that are 
	 * manipulating the distributed collection call this method before it 
	 * returns on any host. This is due to the fact some communication between 
	 * the {@link Place}s in the collection's {@link TeamedPlaceGroup} is needed
	 * to compute/gather the result.
	 * @param result long array in which the result will be gathered
	 */
	public abstract void size(long[] result);

	public TeamOperations(C localObject) {
		handle = localObject;
	}
	
	public void teamedBalance() {
		teamedBalance(new MoveManagerLocal(handle.placeGroup()));
	}

	public void teamedBalance(final float[] balance) {
		teamedBalance(balance, new MoveManagerLocal(handle.placeGroup()));
	}

	public void teamedBalance(final float[] newLocality, final MoveManagerLocal mm) {
		// Rail.copy[Float](ne wL ocality, locality)

		if (newLocality.length != handle.placeGroup().size())
			throw new RuntimeException("[DistCol] the size of newLocality must be the same with placeGroup.size()");
		System.arraycopy(newLocality, 0, handle.locality(), 0, handle.locality().length);
		teamedBalance(mm);
	}

	// TODO
	// maybe these methods should move to the interface like RelocatableCollection or RelocatableMap
	// as default methods.
	public void teamedBalance(MoveManagerLocal mm) {
		final int pgSize = handle.placeGroup().size();
		final IntFloatPair[] listPlaceLocality = new IntFloatPair[pgSize];
		float localitySum = 0.0f;
		long globalDataSize = 0;
		final long[] localDataSize = new long[pgSize];

		for (int i = 0; i < pgSize; i++) {
			localitySum += handle.locality()[i];
		}
		size(localDataSize);

		for (int i = 0; i < pgSize; i++) {
			globalDataSize += localDataSize[i];
			final float normalizeLocality = handle.locality()[i] / localitySum;
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
			handle.placeGroup().barrier(); // for debug print
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
			handle.placeGroup().barrier(); // for debug print
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
			handle.placeGroup().barrier(); // for debug print
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
			handle.placeGroup().barrier(); // for debug print
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
			handle.moveAtSyncCount(moveList.get(handle.placeGroup().myrank), mm);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Error("[AbstractDistCollection] data transfer error raised.");
		}
	}
}
