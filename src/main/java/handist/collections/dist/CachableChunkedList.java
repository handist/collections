package handist.collections.dist;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.ChunkedList;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.Serializer;

import java.io.ObjectStreamException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import static apgas.Constructs.here;

public class CachableChunkedList<T> extends DistCol<T> {

//    static class Team<S> extends TeamOperations<S, CachableChunkedList<S>> {
//
//        /**
//         * Super constructor. Needs to be called by all implementations to initialize
//         * the necessary members common to all Team handles.
//         *
//         * @param localObject local handle of the distributed collection
//         */
//        public Team(CachableChunkedList<S> localObject) {
//            super(localObject);
//        }
//        @Override
//        public void gather(Place root) {
//            throw new UnsupportedOperationException("CachableChunkedList does not support gather().");
//        }
//        @Override
//        public void teamedBalance(CollectiveMoveManager mm) {
//            throw new UnsupportedOperationException("CachableChunkedList does not support balance operations.");
//        }
//    }

    public CachableChunkedList(final TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }
    private CachableChunkedList(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        super(placeGroup, id,
                (TeamedPlaceGroup pg, GlobalID gid)-> new CachableChunkedList<>(pg, gid));
    }

    protected ChunkedList<T> shared = new ChunkedList<>();
    protected HashMap<RangedList<T>,Place> shared2owner = new HashMap<>();

    public ChunkedList<T> sharedChunks() {
        return new ChunkedList.UnmodifiableView(shared);
    }
    public void forEachSharedChunk(LongRange range, Consumer<RangedList<T>> func) {
        shared.forEachChunk(range, func);
    }
    public Place getSharedOwner(RangedList<T> chunk) {
        return shared2owner.get(chunk);
    }
    public void forEachSharedOwner(LongRange range, Consumer<T> func) {
        shared.forEachChunk(range, (RangedList<T> r0) -> {
            if(shared2owner.get(r0).equals(here())) {
                if(!range.contains(r0.getRange())) r0 = r0.subList(range);
                r0.forEach(func);
            }
        });
    }
    public void forEachSharedOwner(LongRange range, LongTBiConsumer<T> func) {
        shared.forEachChunk(range, (RangedList<T> r0) -> {
            if(shared2owner.get(r0).equals(here())) {
                if(!range.contains(r0.getRange())) r0 = r0.subList(range);
                r0.forEach(func);
            }
        });
    }

    private List<RangedList<T>> searchSharedChunks(Place owner, List<LongRange> ranges) {
        ArrayList<RangedList<T>> result = new ArrayList<>();
        for(LongRange range: ranges) {
            shared.forEachChunk(range, (RangedList<T> r0) -> {
                if (shared2owner.get(r0).equals(owner)) {
                    if (range.contains(r0.getRange())) result.add(r0);
                    else result.add(r0.subList(range));
                }
            });
        }
        return result;
    }
    private List<RangedList<T>> searchSharedChunks(List<LongRange> ranges) {
        ArrayList<RangedList<T>> result = new ArrayList<>();
        for(LongRange range: ranges) {
            shared.forEachChunk(range, (RangedList<T> r0) -> {
                if (range.contains(r0.getRange())) result.add(r0);
                else result.add(r0.subList(range));
            });
        }
        return result;
    }

    private List<RangedList<T>> exportLocalChunks(List<LongRange> ranges) {
        // TODO
        // Should we check the overlaps in ranges?
        ArrayList<RangedList<T>> result = new ArrayList<>();
        for(LongRange range: ranges) {
            forEachChunk(range, (RangedList<T> chunk) -> {
                LongRange r0 = chunk.getRange();
                if (range.contains(r0)) {
                    addNewShared(here(), chunk);
                    result.add(chunk);
                } else if (r0.from < range.from && r0.to > range.to) {
                    if (attemptSplitChunkAtTwoPoints(range)) {
                        RangedList<T> c = getChunk(range);
                        addNewShared(here(), c);
                        result.add(c);
                    } else {
                        throw new ConcurrentModificationException();
                    }
                } else {
                    long splitPoint = (r0.from >= range.from) ? range.to : range.from;
                    LongRange rRange = (r0.from >= range.from) ? new LongRange(r0.from, range.to) : new LongRange(range.from, r0.to);
                    if (attemptSplitChunkAtSinglePoint(new LongRange(splitPoint))) {
                        RangedList<T> c = getChunk(rRange);
                        addNewShared(here(), c);
                        result.add(c);
                    } else {
                        throw new ConcurrentModificationException();
                    }
                }
            });
        }
        return result;
    }
    private void addNewShared(Place owner, List<RangedList<T>> chunks) {
        for(RangedList<T> chunk: chunks) {
            if(!owner.equals(here())) this.add(chunk);
            shared.add(chunk);
            shared2owner.put(chunk, owner);
        }
    }
    private void addNewShared(Place owner, RangedList<T> chunk) {
        if(!owner.equals(here())) this.add(chunk);
        shared.add(chunk);
        shared2owner.put(chunk, owner);
    }

    private void assertUnshared(LongRange r) {
        if(!searchSharedChunks(Collections.singletonList(r)).isEmpty()) {
            throw new IllegalStateException("CachableChunkedList found shared chunks in range: " + r);
        }
    }


    /**
     * conduct broadcast operation on chunks that are not shared with other places yet.
     * The user must call each of the share methods of a cachable chunked list in all the place belonging to the place group.
     * This method should not be called simultaneously with other collective methods.
     * The caller place is treated as the owner even if the chunks become shared.
     *
     * Note 1: if you want to share all the local chunks, please call {@code share()}.
     * Note 2: if you want to specify multiple ranges, please use {@code share(List<LongRange>)}.
     * Note 3: if you don't want to share any local chunks from the called place, please specify an empty range or an empty list of ranges.
     * Note 3: if you want to conduct relocate process of multiple cachable chunked lists using the same ObjectOutput(Stream),
     * please prepare an instance of {@link CollectiveRelocator.Allgather} first and
     * call the relocation methods of the cachable chunked lists in the same order specifying the collective relocator as a parameter,
     * and finally call the execute method of the relocator.
     *
     * @param ranges The library scans the ranges and exports (the parts of) the local chunks in the ranges.
     * @param mm You can relocate multiple cachable chunked lists using the same collective relocator, specified with {@code mm}.
     */
    public void share(final List<LongRange> ranges, CollectiveRelocator.Allgather mm) {
        final List<RangedList<T>> chunks = exportLocalChunks(ranges);
        final Serializer serProcess = (ObjectOutput s) -> {
            s.writeObject(chunks);
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place sender) -> {
            final List<RangedList<T>> received = (List<RangedList<T>>) ds.readObject();
            addNewShared(sender, received);
        };
        mm.request(serProcess, desProcess);
    }

    public void share(LongRange range) {
        CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        share(Collections.singletonList(range), mm);
        mm.execute();
    }

    public void share(List<LongRange> ranges) {
        CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        share(ranges, mm);
        mm.execute();
    }

    public void share() {
        CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        share(Collections.singletonList(null), mm);
        mm.execute();
    }

    public void share(LongRange range, CollectiveRelocator.Allgather mm) {
        share(Collections.singletonList(range), mm);
    }

    public void share(CollectiveRelocator.Allgather mm) {
        share(Collections.singletonList(null), mm);
    }



    /**
     * conduct broadcast operation on chunks that are already shared within the place group.
     * The user must call each of the broadcast methods of a cachable chunked list in all the place belonging to the place group.
     *
     * @param ranges
     * @param pack
     * @param unpack
     * @param mm
     * @param <U>
     */
    public <U> void bcast(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        final List<RangedList<T>> chunks = searchSharedChunks(here(), ranges);
        final Serializer serProcess = (ObjectOutput s) -> {
            s.writeObject(ranges);
            for(RangedList<T> chunk: chunks) {
                chunk.forEach((T elem) -> {
                    s.writeObject(pack.apply(elem));
                });
            }
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place p) -> {
            if(p.equals(here())) return;
            List<LongRange> rangesX = (List<LongRange>) ds.readObject();
            List<RangedList<T>> receiving = searchSharedChunks(p, rangesX);
            for(RangedList<T> chunk: receiving) {
                chunk.forEach((T elem) -> {
                    final U diff = (U) ds.readObject();
                    unpack.accept(elem, diff);
                });
            }
        };
        mm.request(serProcess, desProcess);
    }
    public <U> void bcast(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack) {
        CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        bcast(ranges, pack, unpack, mm);
        mm.execute();
    }

    public <U> void bcast(LongRange range, Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        bcast(Collections.singletonList(range), pack, unpack, mm);
    }

    public <U> void bcast(LongRange range, Function<T, U> pack, BiConsumer<T, U> unpack) {
        CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        bcast(range, pack, unpack, mm);
        mm.execute();
    }

    public <U> void bcast(Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        bcast((LongRange) null, pack, unpack, mm);
    }
    public <U> void bcast(Function<T, U> pack, BiConsumer<T, U> unpack) {
        bcast((LongRange) null, pack, unpack);
    }

    /**
     * conduct allreduce operation on shared chunks in the given range.
     * Note: please use the same ranges in all the places.
     *
     * @param ranges the list of ranges in which chunks are applied to the operation.
     * @param pack the function that receives an element and extracts data that will be transferred to other places and be reduced by the unpack operation.
     * @param unpack the function that receives a local element and the transferred data from each place and conducts reduction operation to the local element.
     * @param mm the collective relocator to manage serialize process
     * @param <U> the type of the extracted data
     */
    public <U> void allreduce(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        final List<RangedList<T>> chunks = searchSharedChunks(ranges);
        final Serializer serProcess = (ObjectOutput s) -> {
            for(RangedList<T> chunk: chunks) {
                chunk.forEach((T elem) -> {
                    s.writeObject(pack.apply(elem));
                });
            }
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place place) -> {
            for(RangedList<T> chunk: chunks) {
                chunk.forEach((T elem) -> {
                    @SuppressWarnings("unchecked") final U diff = (U) ds.readObject();
                    unpack.accept(elem, diff);
                });
            }
        };
        mm.request(serProcess, desProcess);
    }
    public <U> void allreduce(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack) {
        CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        allreduce(ranges, pack, unpack, mm);
        mm.execute();
    }
    public <U> void allreduce(Function<T, U> pack, BiConsumer<T, U> unpack) {
        allreduce(new ArrayList<LongRange>(ranges()), pack, unpack);    // TODO: not good, copying ranges to arraylist
    }
    public <U> void allreduce(Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        allreduce(new ArrayList<LongRange>(ranges()), pack, unpack, mm);   // TODO: not good, copying ranges to arraylist
    }




    public <U> void reduce(List<LongRange> ranges, final Function<T, U> pack, final SerializableBiConsumer<T, U> unpack, CollectiveMoveManager mm) {
        final CachableChunkedList<T> toBranch = this;
        for(Place p: placeGroup().places()) {
            if(p.equals(here())) continue;
            final List<RangedList<T>> chunks = searchSharedChunks(p, ranges);
            final Serializer serProcess = (ObjectOutput s) -> {
                s.writeInt(chunks.size());
                for(RangedList<T> chunk: chunks) {
                    s.writeObject(chunk.getRange());
                    chunk.forEach((T elem) -> {
                        s.writeObject(pack.apply(elem));
                    });
                }
            };
            final DeSerializer desProcess = (ObjectInput ds) -> {
                int n = ds.readInt();
                for(int i=0; i<n; i++) {
                    LongRange range0 = (LongRange) ds.readObject();
                    if(!toBranch.containsRange(range0)) {
                        throw new ConcurrentModificationException("The specified range seems to be remove from " + toBranch + " at "+ here());
                    }
                    toBranch.forEach(range0, (T elem) -> {
                        @SuppressWarnings("unchecked") final U diff = (U) ds.readObject();
                        unpack.accept(elem, diff);
                    });
                }
            };
            mm.request(p, serProcess, desProcess);
        }
    }
    public <U> void reduce(List<LongRange> ranges, Function<T, U> pack, SerializableBiConsumer<T, U> unpack) {
        CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup());
        reduce(ranges, pack, unpack, mm);
        try {
            mm.sync();
        } catch(Exception e) {
            e.printStackTrace();
            throw new Error("Exception raised during CachbleArray#reduce().");
        }
    }

    @Override
    public void setProxyGenerator(Function<Long,T> func) {
        throw new UnsupportedOperationException("CachableChunkedList does not support proxy feature.");
    }

    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = manager.placeGroup;
        final GlobalID id1 = id();
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new CachableChunkedList<>(pg1, id1);
        });
    }

    @Override
    protected void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManager mm) {
        // check or filter out shared ones.
        for(RangedList<T> c: cs) assertUnshared(c.getRange());
        super.moveAtSync(cs, dest, mm);
    }


    @Override
    public RangedList<T> remove(final LongRange r) {
        assertUnshared(r);
        return super.remove(r);
    }
    @Override
    public void clear() {
        // TODO
        // The super of clear() assums teamed operation of clear();
    }

    // TODO
    // prepare documents for the following methods
    // getSizedistribution: only returns unshared
    // getDistribution: only returns unshared
    // getRangedDistribution: only returns unshared
    // TEAM: only support DistCol methods

}
