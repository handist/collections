package handist.util.dist;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

import apgas.SerializableCallable;
import apgas.util.GlobalID;


public abstract class AbstractDistCollection implements Serializable {
    private static int _debug_level = 5;

    public static class LazyObjectReference<T> implements Serializable {

        private static final long serialVersionUID = -5737446834905219177L;
        protected final TeamedPlaceGroup pg0;
        protected final GlobalID id0;
        protected SerializableCallable<T> init;

        protected LazyObjectReference(TeamedPlaceGroup pg, GlobalID id, SerializableCallable<T> init) {
            this.id0 = id;
            this.pg0 = pg;
            this.init = init;
        }

        private Object readResolve() throws ObjectStreamException {
            Object result = id0.getHere();
            if (result == null) {
                try {
                    T r = init.call();
                    id0.putHereIfAbsent(r);
                } catch (Exception e) {
                    throw new Error("[Abstract Dist Collection: init should not raise exceptions.");
                }
                return id0.getHere();
            } else {
                return result;
            }
        }

    }

    abstract public Object writeReplace() throws ObjectStreamException;
    // return new LaObjectReference(id, ()->{ new AbstractDistCollection<>());

    public final TeamedPlaceGroup placeGroup; // may be packed into T? or globalID??
    final GlobalID id;
    // @TransientInitExpr(getLocalData())

    public AbstractDistCollection(TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }

    protected AbstractDistCollection(TeamedPlaceGroup pg, GlobalID id) {
        this.id = id;
        this.placeGroup = pg;
        this.locality = new float[pg.size];
        Arrays.fill(locality, 1.0f);
        id.putHere(this);
    }

    // TODO make(pg, init) 系も欲しい
    /*
     * private void readObject(java.io.ObjectInputStream in) throws IOException,
     * ClassNotFoundException { in.defaultReadObject(); this.data = (T)id.getHere();
     * if(data==null) { id.putHereIfAbsent(getInitData()); this.data =
     * (T)id.getHere(); } } private void writeObject(java.io.ObjectOutputStream out)
     * throws IOException { out.defaultWriteObject(); }
     */
    /**
     * Return the PlaceGroup.
     *
     * @return PlaceGroup.
     */
    public TeamedPlaceGroup placeGroup() {
        return placeGroup;
    }

    public abstract void clear();

    // TODO
    // public abstract void integrate(T src);
    public void teamedBalance() {
        teamedBalance(new MoveManagerLocal(placeGroup));
    };

    static class IFPair {
        int first;
        float second;

        public IFPair(int first, float second) {
            this.first = first;
            this.second = second;
        }
    }

    static class ILPair {
        int first;
        long second;

        public ILPair(int first, long second) {
            this.first = first;
            this.second = second;
        }
    }

    transient float[] locality;
    /*
     * Ensure calling updateDist() before balance() balance() should be called in
     * all places
     */

    abstract public void checkDistInfo(long[] result);

    abstract protected void moveAtSyncCount(final ArrayList<ILPair> moveList, final MoveManagerLocal mm)
            throws Exception;

    // TODO
    // maybe these methods should move to the interface like RelocatableCollection or RelocatableMap
    // as default methods.
    public void teamedBalance(MoveManagerLocal mm) {
        final int pgSize = placeGroup.size();
        final IFPair[] listPlaceLocality = new IFPair[pgSize];
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
            listPlaceLocality[i] = new IFPair(i, normalizeLocality);
        }
        Arrays.sort(listPlaceLocality, (IFPair a1, IFPair a2) -> {
            return Float.compare(a1.second, a2.second);
        });

        if (_debug_level > 5) {
            for (IFPair pair : listPlaceLocality) {
                System.out.print("(" + pair.first + ", " + pair.second + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        IFPair[] cumulativeLocality = new IFPair[pgSize];
        float sumLocality = 0.0f;
        for (int i = 0; i < pgSize; i++) {
            sumLocality += listPlaceLocality[i].second;
            cumulativeLocality[i] = new IFPair(listPlaceLocality[i].first, sumLocality);
        }
        cumulativeLocality[pgSize - 1] = new IFPair(listPlaceLocality[pgSize - 1].first, 1.0f);

        if (_debug_level > 5) {
            for (int i = 0; i < pgSize; i++) {
                IFPair pair = cumulativeLocality[i];
                System.out.print("(" + pair.first + ", " + pair.second + ", " + localDataSize[pair.first] + "/"
                        + globalDataSize + ") ");
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        final ArrayList<ArrayList<ILPair>> moveList = new ArrayList<>(pgSize); // ArrayList(index of dest Place, num
                                                                               // data to export)
        ArrayList<ILPair> stagedData = new ArrayList<>(); // ArrayList(index of src, num data to export)
        long previousCumuNumData = 0;

        for (int i = 0; i < pgSize; i++) {
            moveList.add(new ArrayList<ILPair>());
        }

        for (int i = 0; i < pgSize; i++) {
            int placeIdx = cumulativeLocality[i].first;
            float placeLocality = cumulativeLocality[i].second;
            long cumuNumData = (long) (((float) globalDataSize) * placeLocality);
            long targetNumData = cumuNumData - previousCumuNumData;
            if (localDataSize[placeIdx] > targetNumData) {
                stagedData.add(new ILPair(placeIdx, localDataSize[placeIdx] - targetNumData));
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
                    ILPair pair = stagedData.remove(0);
                    if (pair.second > numToImport) {
                        moveList.get(pair.first).add(new ILPair(placeIdx, numToImport));
                        stagedData.add(new ILPair(pair.first, pair.second - numToImport));
                        numToImport = 0;
                    } else {
                        moveList.get(pair.first).add(new ILPair(placeIdx, pair.second));
                        numToImport -= pair.second;
                    }
                }
            }
            previousCumuNumData = cumuNumData;
        }

        if (_debug_level > 5) {
            for (int i = 0; i < pgSize; i++) {
                for (ILPair pair : moveList.get(i)) {
                    System.out.print("src: " + i + " dest: " + pair.first + " size: " + pair.second + ", ");
                }
            }
            System.out.println();
            placeGroup.barrier(); // for debug print
        }

        if (_debug_level > 5) {
            long[] diffNumData = new long[pgSize];
            for (int i = 0; i < pgSize; i++) {
                for (ILPair pair : moveList.get(i)) {
                    diffNumData[i] -= pair.second;
                    diffNumData[pair.first] += pair.second;
                }
            }
            for (IFPair pair : listPlaceLocality) {
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

    public void teamedBalance(final float[] newLocality, final MoveManagerLocal mm) {
        // Rail.copy[Float](ne wL ocality, locality)

        if (newLocality.length != placeGroup.size())
            throw new RuntimeException("[DistCol] the size of newLocality must be the same with placeGroup.size()");
        System.arraycopy(newLocality, 0, locality, 0, locality.length);
        teamedBalance(mm);
    }

    public void teamedBalance(final float[] balance) {
        teamedBalance(balance, new MoveManagerLocal(placeGroup));
    }

    protected void balanceSpecCheck(final float[] balance) {
        if (balance.length != placeGroup.size) {
            throw new RuntimeException("[AbstractDistCollection");
        }        
    }

    public void balance(final float[] balance) {
        balanceSpecCheck(balance);
        TeamedPlaceGroup pg = this.placeGroup;
        AbstractDistCollection handle = this;
        pg.broadcastFlat(() -> {
            handle.teamedBalance(balance);
        });
    }

    public void balance() {
        final TeamedPlaceGroup pg = placeGroup;
        AbstractDistCollection handle = this;
        pg.broadcastFlat(() -> {
            handle.teamedBalance();
        });
    }

    /**
     * Destroy an instance of AbstractDistCollection.
     */
    public void destroy() {
        placeGroup.remove(id);
    }

    /*
    public final def printAllData(){
        for(p in placeGroup){
            at(p){
                printLocalData();
            }
        }
    }*/

}
