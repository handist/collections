package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.ElementOverlapException;
import handist.collections.LongRange;
import handist.collections.LongRangeSet;
import handist.collections.RangedList;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.dist.util.Pair;
import handist.collections.function.DeSerializer;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.Serializer;

/**
 * {@link DistChunkedList} with additional features allowing it to replicate
 * some range of values held by a process to another process
 *
 * @param <T> type contained by this collection
 */
/*
 * Implementation note: This class does not extend DistCol because shared chunks
 * from other places should not register to ldist.
 */
public class CachableDistCol<T> extends DistChunkedList<T>
        implements RangeCachable<LongRange>, ElementLocationManageable<LongRange> {

    /**
     * Internal class that handles distribution-related operations.
     */
    protected final transient ElementLocationManager<LongRange> ldist;
    /**
     * List of chunks that have been shared to this local branch by a remote branch.
     * NOTE: <b>BE CAREFUL</b> not to modify received chunks.(such remove(), ).
     */
    protected final transient ChunkedList<T> received;
    /**
     * Map keeping track of the "owner" of each range in the collection. Registerd
     * even when added locally.
     */
    /*
     * TODO Can we remove this? We can obtain the owner Owner using member #ldist.
     */
    protected final transient HashMap<RangedList<T>, Place> owner;
    /**
     * Map having ranges that were sent to each remote branch
     */
    protected final transient HashMap<Place, LongRangeSet> sentRanges;
    /**
     * Set having ranges that were sent to remote places
     */
    protected final transient LongRangeSet allSentRanges;

    /**
     * Creates a new {@link CachableDistCol} on the specified
     * {@link TeamedPlaceGroup}
     *
     * @param pg the group of places on which this collection may have a branch
     */
    public CachableDistCol(final TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }

    /**
     * Creates a new branch for a {@link CachableDistCol} with the specified group
     * of places and id which identifies the distributed collection into which the
     * created branch is taking place
     *
     * @param placeGroup the group of places on which the distributed collection may
     *                   have a handle
     * @param id         global id identifying the distributed collection
     */
    CachableDistCol(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        super(placeGroup, id, (TeamedPlaceGroup pg, GlobalID gid) -> new CachableDistCol<>(pg, gid));
        received = new ChunkedList<>();
        owner = new HashMap<>();
        sentRanges = new HashMap<>(placeGroup.size());
        allSentRanges = new LongRangeSet();
        ldist = new ElementLocationManager<>();
        for (final Place pl : placeGroup.places()) {
            if (pl == here()) {
                continue;
            }
            sentRanges.put(pl, new LongRangeSet());
        }
    }

    @Override
    public void add(RangedList<T> c) throws ElementOverlapException {
        super.add(c);
        ldist.add(c.getRange());
        owner.put(c, here());
    }

    @Override
    public void add_unchecked(RangedList<T> c) {
        super.add_unchecked(c);
        ldist.add(c.getRange());
        owner.put(c, here());
    }

    /**
     * conduct allreduce operation on shared chunks.
     *
     * @param pack   the function that receives an element and extracts data that
     *               will be transferred to other places and be reduced by the
     *               unpack operation.
     * @param unpack the function that receives a local element and the transferred
     *               data from each place and conducts reduction operation to the
     *               local element.
     * @param <U>    the type of the extracted data
     */
    public <U> void allreduce(Function<T, U> pack, BiConsumer<T, U> unpack) throws Exception {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        allreduce(pack, unpack, mm);
        mm.execute();
    }

    /**
     * conduct allreduce operation on shared chunks.
     *
     * @param pack   the function that receives an element and extracts data that
     *               will be transferred to other places and be reduced by the
     *               unpack operation.
     * @param unpack the function that receives a local element and the transferred
     *               data from each place and conducts reduction operation to the
     *               local element.
     * @param mm     the manager in charge of performing this batch of transfers.
     * @param <U>    the type of the extracted data
     */
    public <U> void allreduce(Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        final Serializer serProcess = (ObjectOutput s) -> {
            // writes chunks owned at here
            allSentRanges.forEach((range) -> {
                forEachChunk(range, (chunk) -> {
                    packAndWrite(pack, chunk.subList(range), s);
                });
            });
            // write chunks owned at remote
            received.forEachChunk((chunk) -> {
                packAndWrite(pack, chunk, s);
            });
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place place) -> {
            while (ds.available() > 0) {
                readAndUnpack(unpack, ds);
            }
        };
        mm.request(serProcess, desProcess);
    }

    /** if the given range overlaps a shared chunks, throw exception. */
    private void assertUnshared(LongRange range) {
        received.forEachChunk(range, (chunk) -> {
            if (chunk.getRange().isOverlapped(range)) {
                throw new IllegalArgumentException("CachableChunkedList found shared chunks in range: " + range
                        + ". shared by " + getOwner(chunk));
            }
        });
        if (allSentRanges.isOverlapped(range)) {
            throw new IllegalArgumentException(
                    "CachableChunkedList found shared chunks in range: " + range + ". shared by " + here());
        }
    }

    /**
     * Conducts a broadcast operation on chunks that are already shared within the
     * place group. The user must call each of the broadcast methods of a cachable
     * chunked list in all the place belonging to the place group.
     *
     * @param <U>    the type used to transfer information from originals to shared
     *               replicas on remote places
     * @param pack   the function used to transform T objects into the U type used
     *               for transfer
     * @param unpack the closure used to update the T objects based on on the
     *               received U objects
     */
    public <U> void bcast(Function<T, U> pack, BiConsumer<T, U> unpack) {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        bcast(pack, unpack, mm);
        mm.execute();
    }

    /**
     * Conducts a broadcast operation on chunks that are already shared within the
     * place group. The user must call each of the broadcast methods of a cachable
     * chunked list in all the place belonging to the place group.
     *
     * @param <U>    the type used to transfer information from originals to shared
     *               replicas on remote places
     * @param pack   the function used to transform T objects into the U type used
     *               for transfer
     * @param unpack the closure used to update the T objects based on on the
     *               received U objects
     * @param mm     the relocator in charge of handling the communication between
     *               hosts
     */
    public <U> void bcast(Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        final Serializer serProcess = (ObjectOutput s) -> {
            // writes chunks owned at here
            allSentRanges.forEach((range) -> {
                forEachChunk(range, (chunk) -> {
                    packAndWrite(pack, chunk.subList(range), s);
                });
            });
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place place) -> {
            while (ds.available() > 0) {
                readAndUnpack(unpack, ds);
            }
        };
        mm.request(serProcess, desProcess);
    }

    @Override
    public void clear() {
        // TODO Should be teamed operation ?
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /**
     * Returns a new {@link ChunkedList} which contains the same {@link Chunk}s as
     * this local instance.
     *
     * @return a ChunkedList which holds the same Chunks as this instance
     */
    @Override
    protected Object clone() {
        return super.clone();
    }

    /**
     * A method for shareRangeAtSync. Pass over chunks overlapping the given ranges
     * for sharing to remote.
     *
     * @param result to which extracted chunks in the given range with each owner
     *               add.
     */
    private void extractChunksToShare(LongRange range, Place destination, HashMap<RangedList<T>, Place> result) {
        forEachChunk(range, (chunk) -> {
            if (getOwner(chunk).equals(destination)) {
                throw new ElementOverlapException(range + " (which owner is " + destination + ") could not share to "
                        + destination + " from " + here());
            }
            // put result
            final Place owner = getOwner(chunk);
            final RangedList<T> view = chunk.subList(range);
            result.put(view, owner);
            // record sending range. If the owner is an other place, the range will be
            // recorded when updateShared()
            if (getOwner(chunk).equals(here())) {
                sentRanges.get(destination).add(chunk.getRange());
                allSentRanges.addCombine(chunk.getRange());
            }
        });
    }

    /**
     * Returns ${link ChunkedList} contains received chunks from given place.
     *
     * @param place to find chunks which owner is that place.
     * @return received chunks from given place. if no received, return empty
     *         chunkedlist.
     */
    public ChunkedList<T> findReceivedFrom(Place place) {
        final ChunkedList<T> result = new ChunkedList<>();
        forEachReceivedChunk((chunk) -> {
            if (getOwner(chunk).equals(place)) {
                result.add(chunk);
            }
        });
        return result;
    }

    public ChunkedList<T> findSentTo(Place place) {
        final ChunkedList<T> result = new ChunkedList<>();
        sentRanges.get(place).forEach((r) -> {
            forEachChunk(r, (chunk) -> {
                result.add(chunk.subList(r));
            });
        });
        return result;
    }

    public void forEachOwn(LongRange range, LongTBiConsumer<T> func) {
        forEachChunk(range, (chunk) -> {
            if (received.containsChunk(chunk)) { // NOTE: If possible, want to avoid the waste loop
                return;
            }
            chunk.subList(range).forEach((i, t) -> {
                func.accept(i, t);
            });
        });
    }

    public void forEachOwn(LongTBiConsumer<T> func) {
        forEachChunk((chunk) -> {
            if (received.containsChunk(chunk)) { // NOTE: If possible, want to avoid the waste loop
                return;
            }
            chunk.forEach((i, t) -> {
                func.accept(i, t);
            });
        });
    }

    public void forEachOwnChunk(Consumer<RangedList<T>> func) {
        forEachChunk((chunk) -> {
            if (received.containsChunk(chunk)) { // NOTE: If possible, want to avoid the waste loop
                return;
            }
            func.accept(chunk);
        });
    }

    public void forEachOwnChunk(LongRange range, Consumer<RangedList<T>> func) {
        forEachChunk(range, (chunk) -> {
            if (received.containsChunk(chunk)) { // NOTE: If possible, want to avoid the waste loop
                return;
            }
            func.accept(chunk);
        });
    }

    public void forEachReceived(LongRange range, LongTBiConsumer<T> func) {
        received.forEach(range, func);
    }

    public void forEachReceived(LongTBiConsumer<T> func) {
        received.forEach(func);
    }

    public void forEachReceivedChunk(Consumer<RangedList<T>> func) {
        received.forEachChunk(func);
    }

    public void forEachReceivedChunk(LongRange range, Consumer<RangedList<T>> func) {
        received.forEachChunk(range, func);
    }

    public void forEachSent(LongRange range, LongTBiConsumer<T> func) {
        allSentRanges.intersections(range).forEach((r) -> {
            forEach(r, func);
        });
    }

    public void forEachSent(LongTBiConsumer<T> func) {
        allSentRanges.forEach((r) -> {
            forEach(r, func);
        });
    }

    public void forEachSentChunk(Consumer<RangedList<T>> func) {
        allSentRanges.forEach((r) -> {
            forEachChunk(r, (chunk) -> {
                func.accept(chunk.subList(r));
            });
        });
    }

    public void forEachSentChunk(LongRange range, Consumer<RangedList<T>> func) {
        allSentRanges.intersections(range).forEach((r) -> {
            forEachChunk(r, (chunk) -> {
                func.accept(chunk.subList(r));
            });
        });
    }

    /**
     * Returns a place where a given chunk is owned.
     *
     * @param chunk to find owner place.
     * @return a place a place where a given chunk is owned.
     */
    public Place getOwner(RangedList<T> chunk) {
        return owner.get(chunk);
    }

    public LongRangeSet getSentRanges(Place dest) {
        return sentRanges.get(dest);
    }

    @Override
    public void getSizeDistribution(long[] result) {
        for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
            final LongRange k = entry.getKey();
            final Place p = entry.getValue();
            result[manager.placeGroup.rank(p)] += k.size();
        }
    }

    /** Not supported */
    @Deprecated
    @Override
    public void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManager mm) throws Exception {
        throw new UnsupportedOperationException("Not supported. Instead please use shareRangeAtSync");
    }

    /** Not supported */
    @Deprecated
    @Override
    public void moveRangeAtSync(Function<LongRange, List<Pair<Place, LongRange>>> rule, CollectiveMoveManager mm) {
        throw new UnsupportedOperationException("Not supported. Instead please use shareRangeAtSync");
    }

    /** Not supported */
    @Deprecated
    @Override
    public void moveRangeAtSync(final LongRange range, final Place dest, final MoveManager mm) {
        throw new UnsupportedOperationException("Not supported. Instead please use shareRangeAtSync");
    }

    /** Not supported */
    @Deprecated
    @Override
    public void moveRangeAtSync(final RangedDistribution<LongRange> rangedDistribution, final CollectiveMoveManager mm)
            throws Exception {
        throw new UnsupportedOperationException("Not supported. Instead please use shareRangeAtSync");
    }

    /**
     * Method for collective communication process such allreduce, bcast. Serialize
     * the provided RangedList elements through packing operation.
     */
    private <U> void packAndWrite(Function<T, U> pack, RangedList<T> target, ObjectOutput out) {
        out.writeObject(target.getRange());
        target.forEach((T t) -> {
            out.writeObject(pack.apply(t));
        });
    }

    /**
     * Method for collective communication process such allreduce, bcast.
     * Deserialize one RangedList and unpack to local elements. If that LongRange
     * does not exist local, wasted "readObject" loop by that amount.
     */
    @SuppressWarnings("unchecked")
    private <U> void readAndUnpack(BiConsumer<T, U> unpack, ObjectInput in) {
        final LongRange range = (LongRange) in.readObject();
        final long[] index = { range.from }; // make array in order to use forEach
        forEachChunk(range, (chunk) -> {
            final RangedList<T> target = chunk.subList(range);
            // wasted "readObject" loop
            while (index[0] < target.getRange().from) {
                in.readObject();
                index[0]++;
            }
            // unpack
            target.forEach((long i, T t) -> {
                final U u = (U) in.readObject();
                unpack.accept(t, u);
                index[0]++;
            });
        });
        // wasted "readObject" loop
        while (index[0] < range.to) {
            in.readObject();
            index[0]++;
        }
    }

    @Override
    public void registerDistribution(UpdatableDistribution<LongRange> distributionToUpdate) {
        ldist.registerDistribution(distributionToUpdate);
    }

    @Override
    public RangedList<T> remove(LongRange r) {
        assertUnshared(r);
        ldist.remove(r);
        return super.remove(r);
    }

    @Override
    public RangedList<T> remove(RangedList<T> c) {
        return this.remove(c.getRange());
    }

    /**
     * Adds a ranged list shared by a remote branch to this local branch
     *
     * @param owner the owner of the shared ranged list
     * @param chunk the shared with this branch
     */
    private void sharedFrom(Place owner, RangedList<T> chunk) {
        if (owner.equals(here())) {
            return;
        }
        super.add(chunk);
        received.add(chunk);
        this.owner.put(chunk, owner);
    }

    @Override
    public void shareRangeAtSync(Collection<LongRange> ranges, Place destination, MoveManager mm) {
        if (destination == here()) {
            return;
        }
        final Serializer serProcess = (ObjectOutput s) -> {
            final HashMap<RangedList<T>, Place> chunksOwner = new HashMap<>();
            for (final LongRange range : ranges) {
                extractChunksToShare(range, destination, chunksOwner);
            }
            s.writeInt(chunksOwner.size());
            chunksOwner.forEach((chunk, owner) -> {
                s.writeObject(chunk);
                s.writeObject(owner);
            });
        };
        final CachableDistCol<T> toBranch = this;
        @SuppressWarnings("unchecked")
        final DeSerializer desProcess = (ObjectInput ds) -> {
            final int size = ds.readInt();
            for (int i = 0; i < size; i++) {
                final RangedList<T> chunk = (RangedList<T>) ds.readObject();
                final Place owner = (Place) ds.readObject();
                toBranch.sharedFrom(owner, chunk);
            }
        };
        mm.request(destination, serProcess, desProcess);
    }

    @Override
    public void shareRangeAtSync(LongRange range, Place destination, MoveManager mm) {
        if (destination == here()) {
            return;
        }
        final Serializer serProcess = (ObjectOutput s) -> {
            final HashMap<RangedList<T>, Place> chunksOwner = new HashMap<>();
            extractChunksToShare(range, destination, chunksOwner);
            s.writeInt(chunksOwner.size());
            chunksOwner.forEach((chunk, owner) -> {
                s.writeObject(chunk);
                s.writeObject(owner);
            });
        };
        final CachableDistCol<T> toBranch = this;
        @SuppressWarnings("unchecked")
        final DeSerializer desProcess = (ObjectInput ds) -> {
            final int size = ds.readInt();
            for (int i = 0; i < size; i++) {
                final RangedList<T> chunk = (RangedList<T>) ds.readObject();
                final Place owner = (Place) ds.readObject();
                toBranch.sharedFrom(owner, chunk);
            }
        };
        mm.request(destination, serProcess, desProcess);
    }

    @Override
    public ArrayList<RangedList<T>> splitChunks(LongRange range) {
        assertUnshared(range);
        return super.splitChunks(range);
    }

    @Override
    public void updateDist() {
        ldist.update(manager.placeGroup);
    }

    public void updateShared() throws Exception {
        final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup());
        for (final Place place : placeGroup().places()) {
            if (place.equals(here())) {
                continue;
            }
            final Serializer ser = (s) -> {
                final Collection<LongRange> ranges = findReceivedFrom(place).ranges();
                s.writeInt(ranges.size());
                for (final LongRange r : ranges) {
                    s.writeObject(r);
                }
            };
            final Place sender = here();
            final DeSerializer des = (ds) -> {
                final int size = ds.readInt();
                for (int i = 0; i < size; i++) {
                    final LongRange r = (LongRange) ds.readObject();
                    sentRanges.get(sender).add(r);
                    allSentRanges.add(r);
                }
            };
            mm.request(place, ser, des);
        }
        mm.sync();
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = manager.placeGroup;
        final GlobalID id1 = id();
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new CachableDistCol<>(pg1, id1);
        });
    }
}
