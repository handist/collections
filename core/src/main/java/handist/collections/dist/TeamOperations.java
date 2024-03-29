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

import java.util.ArrayList;
import java.util.Arrays;

import apgas.Place;
import handist.collections.dist.util.IntFloatPair;
import handist.collections.dist.util.IntLongPair;
import mpi.MPI;
import mpi.MPIException;

/**
 * Class which defines the "team" operations that distributed collections
 * provide.
 *
 * @param <T> type of the objects contained in the distributed collection
 * @param <C> type of the local handle on which the TeamOperations operate
 */
public class TeamOperations<T, C extends DistributedCollection<T, C>> {

    static int _debug_level = 5;

    protected final C handle;

    /**
     * Super constructor. Needs to be called by all implementations to initialize
     * the necessary members common to all Team handles.
     *
     * @param localObject local handle of the distributed collection
     */
    public TeamOperations(C localObject) {
        handle = localObject;
    }

    public void gather(Place destination) {
        // TODO not implemented yet
    }

    /**
     * Computes and gathers the size of each local collection into the provided
     * array. This operation usually requires that all the hosts that are
     * manipulating the distributed collection call this method before it returns on
     * any host. This is due to the fact some communication between the
     * {@link Place}s in the collection's {@link TeamedPlaceGroup} is needed to
     * compute/gather the result.
     *
     * @param result long array in which the result will be gathered
     */
    @SuppressWarnings({ "rawtypes", "deprecation" })
    public void getSizeDistribution(final long[] result) {
        if (handle instanceof ElementLocationManageable) {
            ((ElementLocationManageable) handle).getSizeDistribution(result);
            return;
        }
        final TeamedPlaceGroup pg = handle.placeGroup();
        result[pg.myrank] = handle.longSize();
        try {
            // THIS WORKS FOR MPJ-NATIVE implementation
            // There appears to be a bug in the mpiJava implementation
            pg.comm.Allgather(result, pg.myrank, 1, MPI.LONG, result, 0, 1, MPI.LONG);
        } catch (final MPIException e) {
            e.printStackTrace();
            throw new Error("[DistMap] network error in team().size()");
        }
    }

    public void teamedBalance() {
        teamedBalance(new CollectiveMoveManager(handle.placeGroup()));
    }

    /**
     * Redistributes all the entries of the underlying distributed collection
     * between the places.
     *
     * @param mm move manager in charge of the transfer
     */
    /*
     * TODO maybe these methods should move to the interface like
     * RelocatableCollection or RelocatableMap as default methods.
     */
    public void teamedBalance(CollectiveMoveManager mm) {
        final int pgSize = handle.placeGroup().size();
        final IntFloatPair[] listPlaceLocality = new IntFloatPair[pgSize];
        float localitySum = 0.0f;
        long globalDataSize = 0;
        final long[] localDataSize = new long[pgSize];

        for (int i = 0; i < pgSize; i++) {
            localitySum += handle.locality()[i];
        }
        getSizeDistribution(localDataSize);

        for (int i = 0; i < pgSize; i++) {
            globalDataSize += localDataSize[i];
            final float normalizeLocality = handle.locality()[i] / localitySum;
            listPlaceLocality[i] = new IntFloatPair(i, normalizeLocality);
        }
        Arrays.sort(listPlaceLocality, (IntFloatPair a1, IntFloatPair a2) -> {
            return Float.compare(a1.second, a2.second);
        });

        if (_debug_level > 5) {
            for (final IntFloatPair pair : listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ") ");
            }
            System.out.println();
            handle.placeGroup().barrier(); // for debug print
        }

        final IntFloatPair[] cumulativeLocality = new IntFloatPair[pgSize];
        float sumLocality = 0.0f;
        for (int i = 0; i < pgSize; i++) {
            sumLocality += listPlaceLocality[i].second;
            cumulativeLocality[i] = new IntFloatPair(listPlaceLocality[i].first, sumLocality);
        }
        cumulativeLocality[pgSize - 1] = new IntFloatPair(listPlaceLocality[pgSize - 1].first, 1.0f);

        if (_debug_level > 5) {
            for (int i = 0; i < pgSize; i++) {
                final IntFloatPair pair = cumulativeLocality[i];
                System.out.print("(" + pair.first + ", " + pair.second + ", " + localDataSize[pair.first] + "/"
                        + globalDataSize + ") ");
            }
            System.out.println();
            handle.placeGroup().barrier(); // for debug print
        }

        final ArrayList<ArrayList<IntLongPair>> moveList = new ArrayList<>(pgSize); // ArrayList(index of dest Place,
        // num
        // data to export)
        final ArrayList<IntLongPair> stagedData = new ArrayList<>(); // ArrayList(index of src, num data to export)
        long previousCumuNumData = 0;

        for (int i = 0; i < pgSize; i++) {
            moveList.add(new ArrayList<IntLongPair>());
        }

        for (int i = 0; i < pgSize; i++) {
            final int placeIdx = cumulativeLocality[i].first;
            final float placeLocality = cumulativeLocality[i].second;
            final long cumuNumData = (long) ((globalDataSize) * placeLocality);
            final long targetNumData = cumuNumData - previousCumuNumData;
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
            final int placeIdx = cumulativeLocality[i].first;
            final float placeLocality = cumulativeLocality[i].second;
            final long cumuNumData = (long) ((globalDataSize) * placeLocality);
            final long targetNumData = cumuNumData - previousCumuNumData;
            if (targetNumData > localDataSize[placeIdx]) {
                long numToImport = targetNumData - localDataSize[placeIdx];
                while (numToImport > 0) {
                    final IntLongPair pair = stagedData.remove(0);
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
                for (final IntLongPair pair : moveList.get(i)) {
                    System.out.print("src: " + i + " dest: " + pair.first + " size: " + pair.second + ", ");
                }
            }
            System.out.println();
            handle.placeGroup().barrier(); // for debug print
        }

        if (_debug_level > 5) {
            final long[] diffNumData = new long[pgSize];
            for (int i = 0; i < pgSize; i++) {
                for (final IntLongPair pair : moveList.get(i)) {
                    diffNumData[i] -= pair.second;
                    diffNumData[pair.first] += pair.second;
                }
            }
            for (final IntFloatPair pair : listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ", "

                        + (localDataSize[pair.first] + diffNumData[pair.first]) + "/" + globalDataSize + ") ");
            }
            System.out.println();
        }

        try {
            handle.moveAtSyncCount(moveList.get(handle.placeGroup().myrank), mm);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new Error("[AbstractDistCollection] data transfer error raised.");
        }
    }

    public void teamedBalance(final float[] balance) {
        teamedBalance(balance, new CollectiveMoveManager(handle.placeGroup()));
    }

    public void teamedBalance(final float[] newLocality, final CollectiveMoveManager mm) {
        if (newLocality.length != handle.placeGroup().size()) {
            throw new RuntimeException("[DistCol] the size of newLocality must be the same with placeGroup.size()");
        }
        System.arraycopy(newLocality, 0, handle.locality(), 0, handle.locality().length);
        teamedBalance(mm);
    }

    /**
     * Conduct element location management process if the target is
     * ElementLocationManagable
     */
    @SuppressWarnings("rawtypes")
    public void updateDist() {
        if (handle instanceof ElementLocationManageable) {
            ((ElementLocationManageable) handle).updateDist();
        }
    }

}
