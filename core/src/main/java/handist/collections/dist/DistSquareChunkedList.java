package handist.collections.dist;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.function.BiFunction;

import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.SquareChunkedList;
import handist.collections.dist.util.IntLongPair;
import handist.collections.function.SerializableConsumer;

public class DistSquareChunkedList<T> extends SquareChunkedList<T>
        implements DistributedCollection<T, DistSquareChunkedList<T>>, SerializableWithReplace {

    public DistSquareChunkedList() {
        this(TeamedPlaceGroup.getWorld());
    }

    public DistSquareChunkedList(final TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    private DistSquareChunkedList(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        this(placeGroup, id, (TeamedPlaceGroup pg, GlobalID gid) -> new DistSquareChunkedList<>(pg, gid));
    }

    protected DistSquareChunkedList(final TeamedPlaceGroup placeGroup, final GlobalID id,
            BiFunction<TeamedPlaceGroup, GlobalID, ? extends DistSquareChunkedList<T>> lazyCreator) {
        super();
        id.putHere(this);
    }

    @Override
    public void forEach(SerializableConsumer<T> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public <S extends DistCollectionSatellite<DistSquareChunkedList<T>, S>> S getSatellite() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GlobalOperations<T, DistSquareChunkedList<T>> global() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public GlobalID id() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public float[] locality() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long longSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void moveAtSyncCount(ArrayList<IntLongPair> moveList, MoveManager mm) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void parallelForEach(SerializableConsumer<T> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <S extends DistCollectionSatellite<DistSquareChunkedList<T>, S>> void setSatellite(S satellite) {
        // TODO Auto-generated method stub

    }

    @Override
    public TeamOperations<T, DistSquareChunkedList<T>> team() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        // TODO Auto-generated method stub
        return null;
    }

}
