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
import java.util.ArrayList;
import java.util.Arrays;

import apgas.util.GlobalID;
import handist.collections.dist.util.IntFloatPair;
import handist.collections.dist.util.IntLongPair;

/**
 * General Distribution Manager which handles the entries of a distributed
 * collection based on their number
 *
 * @param <T> the type of the local handle of the distributed collection
 */
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
     * public GeneralDistManager(TeamedPlaceGroup pg, T branch) { this(pg, new
     * GlobalID(), branch); }
     */

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
        final TeamedPlaceGroup pg = this.placeGroup;
        final GeneralDistManager<T> handle = this;
        pg.broadcastFlat(() -> {
            handle.teamedBalance(balance);
        });
    }

    protected void balanceSpecCheck(final float[] balance) {
        if (balance.length != placeGroup.size) {
            throw new RuntimeException("[AbstractDistCollection");
        }
    }

    @SuppressWarnings("rawtypes")
    protected void checkDistInfo(long[] result) {
        if (branch instanceof ElementLocationManagable) {
            ((ElementLocationManagable) branch).getSizeDistribution(result);
            return;
        } else {
            // TODO
            throw new UnsupportedOperationException("not implemented yet");
        }
    }

    /**
     * Destroy an instance of AbstractDistCollection.
     */
    public void destroy() {
        placeGroup.remove(id);
    }

    abstract protected void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final CollectiveMoveManager mm)
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
        teamedBalance(new CollectiveMoveManager(placeGroup));
    }

    // TODO
    // maybe these methods should move to the interface like RelocatableCollection
    // or RelocatableMap
    // as default methods.
    public void teamedBalance(CollectiveMoveManager mm) {
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
            for (final IntFloatPair pair : listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
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
            placeGroup.barrier(); // for debug print
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
            placeGroup.barrier(); // for debug print
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
            placeGroup.barrier(); // for debug print
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
            moveAtSyncCount(moveList.get(placeGroup.myrank), mm);
        } catch (final Exception e) {
            e.printStackTrace();
            throw new Error("[AbstractDistCollection] data transfer error raised.");
        }
    }

    public void teamedBalance(final float[] balance) {
        teamedBalance(balance, new CollectiveMoveManager(placeGroup));
    }

    public void teamedBalance(final float[] newLocality, final CollectiveMoveManager mm) {
        // Rail.copy[Float](ne wL ocality, locality)

        if (newLocality.length != placeGroup.size()) {
            throw new RuntimeException("[DistCol] the size of newLocality must be the same with placeGroup.size()");
        }
        System.arraycopy(newLocality, 0, locality, 0, locality.length);
        teamedBalance(mm);
    }

    // abstract public Object writeReplace() throws ObjectStreamException;
    // return new LaObjectReference(id, ()->{ new AbstractDistCollection<>());

    /*
     * public final def printAllData(){ for(p in placeGroup){ at(p){
     * printLocalData(); } } }
     */

}
