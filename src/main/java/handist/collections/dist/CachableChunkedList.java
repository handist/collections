package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.PrimitiveInput;
import handist.collections.function.PrimitiveOutput;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.Serializer;
import mpi.MPI;
import mpi.MPIException;
import mpi.Op;

/**
 * {@link DistCol} with additional features allowing it to replicate some range
 * of values held by a process onto other processes
 *
 * @param <T> type contained by this collection
 */
public class CachableChunkedList<T> extends DistCol<T> {

//    public static class Team<T> extends DistCol.Team<T> {
//
//        /**
//         * Super constructor. Needs to be called by all implementations to initialize
//         * the necessary members common to all Team handles.
//         *
//         * @param localObject local handle of the distributed collection
//         */
//        private Team(CachableChunkedList<T> localObject) {
//            super(localObject);
//        }
//
//        @Override
//        public void gather(Place root) {
//            throw new UnsupportedOperationException("CachableChunkedList does not support gather().");
//        }
//
//        /**
//         * Computes and gathers the size of each local collection <b>not including
//         * shared collections</b> into the provided array. This operation usually
//         * requires that all the hosts that are manipulating the distributed collection
//         * call this method before it returns on any host. This is due to the fact some
//         * communication between the {@link Place}s in the collection's
//         * {@link TeamedPlaceGroup} is needed to compute/gather the result.
//         *
//         * @param result long array in which the result will be gathered
//         */
//        @Override
//        public void getSizeDistribution(final long[] result) {
//            super.getSizeDistribution(result);
//        }
//
//        @Override
//        public <R extends Reducer<R, T>> R parallelReduce(R reducer) {
//            return super.parallelReduce(reducer);
//        }
//
//        @Override
//        public <R extends Reducer<R, T>> R reduce(R reducer) {
//            return super.reduce(reducer);
//        }
//
//        @Override
//        public void teamedBalance() {
//            throw new UnsupportedOperationException("CachableChunkedList does not support balance operations.");
//        }
//
//        @Override
//        public void teamedBalance(final CollectiveMoveManager mm) {
//            throw new UnsupportedOperationException("CachableChunkedList does not support balance operations.");
//        }
//
//        @Override
//        public void teamedBalance(final float[] newLocality) {
//            throw new UnsupportedOperationException("CachableChunkedList does not support balance operations.");
//        }
//
//        @Override
//        public void teamedBalance(final float[] newLocality, final CollectiveMoveManager mm) {
//            throw new UnsupportedOperationException("CachableChunkedList does not support balance operations.");
//        }
//    }

    /**
     * List of chunks that have been shared to this local branch by a remote branch
     */
    protected ChunkedList<T> shared = new ChunkedList<>();
    /**
     * Map keeping track of the "owner" of each range in the collection
     */
    protected HashMap<RangedList<T>, Place> shared2owner = new HashMap<>();

    /**
     * Creates a new {@link CachableChunkedList} on the specified
     * {@link TeamedPlaceGroup}
     *
     * @param pg the group of places on which this collection may have a branch
     */
    public CachableChunkedList(final TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }

    /**
     * Creates a new branch for a {@link CachableChunkedList} with the specified
     * group of places and id which identifies the distributed collection into which
     * the created branch is taking place
     *
     * @param placeGroup the group of places on which the distributed collection may
     *                   have a handle
     * @param id         global id identifying the distributed collection
     */
    private CachableChunkedList(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        super(placeGroup, id, (TeamedPlaceGroup pg, GlobalID gid) -> new CachableChunkedList<>(pg, gid));
    }

    /**
     * Adds several ranged lists shared by a remote branch to this local branch
     *
     * @param owner  the owner of the shared ranged lists
     * @param chunks the chunks shared with this branch
     */
    private void addNewShared(Place owner, List<RangedList<T>> chunks) {
        for (final RangedList<T> chunk : chunks) {
            if (!owner.equals(here())) {
                add(chunk);
            }
            shared.add(chunk);
            shared2owner.put(chunk, owner);
        }
    }

    /**
     * Adds a ranged list shared by a remote branch to this local branch
     *
     * @param owner the owner of the shared ranged list
     * @param chunk the shared with this branch
     */
    private void addNewShared(Place owner, RangedList<T> chunk) {
        if (!owner.equals(here())) {
            add(chunk);
        }
        shared.add(chunk);
        shared2owner.put(chunk, owner);
    }

    /**
     * Conduct allreduce operation on shared chunks using MPI reduce operation.
     * <p>
     * This variant cannot handle object data, but is faster than
     * {@link #allreduce(Function, BiConsumer)} in many cases.
     * <p>
     * The core idea consists in converting each individual T object into a number
     * of {@code long}, {@code int}, and {@code double}, perform an MPI primitive
     * "all reduce" reduction on these raw types, and modify the T elements
     * contained by the {@link CachableChunkedList} based on the resulting
     * {@link PrimitiveInput}.
     * <p>
     * The number of raw type values of each type stored into the
     * {@link PrimitiveOutput} must be the same for all T elements. The
     * {@code unpack} closure supplied as second parameter must also extract the
     * same number of raw type data (even if such raw type data is eventually unused
     * to modify the T element) to preserve the consistency of the data in relation
     * to the individual T element being processed.
     *
     * <br>
     * =========================================================================
     * <br>
     * code sample
     *
     * <pre>
     * class Element {
     *     double d1, d2;
     *     int i;
     * }
     *
     * cachableChunkedList.allreduce((PrimitiveOutput out, Element e) -> {
     *     out.writeDouble(e.d1);
     *     out.writeDouble(e.d2);
     *     out.writeInt(e.i);
     * }, (PrimitiveInput in, Element e) -> {
     *     e.d1 = in.readDouble(); // Match the unpack order with the pack closure above
     *     e.d2 = in.readDouble();
     *     in.readInt(); // (For examples purposes) the int is eventually not used to modify `e`,
     *                   // but in.readInt needs to be called regardless
     * }, MPI.SUM);
     * </pre>
     *
     * <br>
     * =========================================================================
     * <br>
     *
     * @param pack   the function that receives an element and extracts data to
     *               transfer and reduce with other places.
     * @param unpack the function that receives a local element and raw data that
     *               was reduced between the hosts using MPI.
     * @param op     the MPI reduction operation used to merge the
     */
    public void allreduce(BiConsumer<PrimitiveOutput, T> pack, BiConsumer<PrimitiveInput, T> unpack, Op op) {
        allreduce(new ArrayList<>(shared.ranges()), pack, unpack, op); // TODO: not good, copying ranges to arraylist
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
    public <U> void allreduce(Function<T, U> pack, BiConsumer<T, U> unpack) {
        allreduce(new ArrayList<>(shared.ranges()), pack, unpack); // TODO: not good, copying ranges to arraylist
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
     * @param mm     the collective relocator to manage serialize process
     * @param <U>    the type of the extracted data
     */
    public <U> void allreduce(Function<T, U> pack, BiConsumer<T, U> unpack, CollectiveRelocator.Allgather mm) {
        allreduce(new ArrayList<>(shared.ranges()), pack, unpack, mm); // TODO: not good, copying ranges to arraylist
    }

    /**
     * Refer to {@link CachableChunkedList#allreduce(BiConsumer, BiConsumer, Op)}.
     * <p>
     * This method needs to be called with the same ranges on all places on which
     * this {@link CachableChunkedList} is defined. Otherwise it will throw
     * {@link MPIException}.
     *
     * @param ranges the ranges on which the common reduction is to be applied
     * @throws MPIException if called with different ranges on the various hosts
     *                      involved in the common reduction
     */
    @SuppressWarnings("deprecation")
    public void allreduce(List<LongRange> ranges, BiConsumer<PrimitiveOutput, T> pack,
            BiConsumer<PrimitiveInput, T> unpack, Op op) {
        final List<RangedList<T>> chunks = searchSharedChunks(ranges);
        final Iterator<RangedList<T>> listIt = chunks.iterator();
        Iterator<T> chunkIt = listIt.next().iterator();
        final PrimitiveStream stream = new PrimitiveStream(10);

        // Count how many times one pack calls writeDouble, writeInt, writeLong.
        pack.accept(stream, chunkIt.next());
        // Compute how many T elements there are to pack
        int nbOfElements = 0;
        for (final RangedList<T> r : chunks) {
            nbOfElements += r.size();
        }
        // Adjust stream size according to how many elements are expected
        stream.adjustSize(nbOfElements);

        // Process the remainder of the elements ...
        // Complete the current chunk
        while (chunkIt.hasNext()) {
            pack.accept(stream, chunkIt.next());
        }
        // Deal with all the remaining chunks in the same manner
        while (listIt.hasNext()) {
            chunkIt = listIt.next().iterator();
            while (chunkIt.hasNext()) {
                pack.accept(stream, chunkIt.next());
            }
        }

        stream.checkIsFull(); // Sanity check, the arrays inside `stream` should be full.

        // communicate
        if (stream.doubleArray.length != 0) {
            final int size = stream.doubleArray.length;
            placeGroup().comm.Allreduce(stream.doubleArray, 0, stream.doubleArray, 0, size, MPI.DOUBLE, op);
        }
        if (stream.intArray.length != 0) {
            final int size = stream.intArray.length;
            placeGroup().comm.Allreduce(stream.intArray, 0, stream.intArray, 0, size, MPI.INT, op);
        }
        if (stream.longArray.length != 0) {
            final int size = stream.longArray.length;
            placeGroup().comm.Allreduce(stream.longArray, 0, stream.longArray, 0, size, MPI.LONG, op);
        }

        // do unpack
        stream.reset();
        for (final RangedList<T> chunk : chunks) {
            chunk.forEach((T, t) -> {
                unpack.accept(stream, t);
            });
        }
    }

    /**
     * conduct allreduce operation on shared chunks in the given range. Note: please
     * use the same ranges in all the places.
     *
     * @param ranges the list of ranges in which chunks are applied to the
     *               operation.
     * @param pack   the function that receives an element and extracts data that
     *               will be transferred to other places and be reduced by the
     *               unpack operation.
     * @param unpack the function that receives a local element and the transferred
     *               data from each place and conducts reduction operation to the
     *               local element.
     * @param <U>    the type of the extracted data
     */
    public <U> void allreduce(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack) {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        allreduce(ranges, pack, unpack, mm);
        mm.execute();
    }

    /**
     * conduct allreduce operation on shared chunks in the given range. Note: please
     * use the same ranges in all the places.
     *
     * @param ranges the list of ranges in which chunks are applied to the
     *               operation.
     * @param pack   the function that receives an element and extracts data that
     *               will be transferred to other places and be reduced by the
     *               unpack operation.
     * @param unpack the function that receives a local element and the transferred
     *               data from each place and conducts reduction operation to the
     *               local element.
     * @param mm     the collective relocator to manage serialize process
     * @param <U>    the type of the extracted data
     */
    public <U> void allreduce(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack,
            CollectiveRelocator.Allgather mm) {
        final List<RangedList<T>> chunks = searchSharedChunks(ranges);
        final Serializer serProcess = (ObjectOutput s) -> {
            for (final RangedList<T> chunk : chunks) {
                chunk.forEach((T elem) -> {
                    s.writeObject(pack.apply(elem));
                });
            }
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place place) -> {
            for (final RangedList<T> chunk : chunks) {
                chunk.forEach((T elem) -> {
                    @SuppressWarnings("unchecked")
                    final U diff = (U) ds.readObject();
                    unpack.accept(elem, diff);
                });
            }
        };
        mm.request(serProcess, desProcess);
    }

    private void assertUnshared(LongRange r) {
        if (!searchSharedChunks(Collections.singletonList(r)).isEmpty()) {
            throw new IllegalStateException("CachableChunkedList found shared chunks in range: " + r);
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
        bcast((LongRange) null, pack, unpack);
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
        bcast((LongRange) null, pack, unpack, mm);
    }

    /**
     * Conducts a broadcast operation on chunks that are already shared within the
     * place group. The user must call each of the broadcast methods of a cachable
     * chunked list in all the place belonging to the place group.
     *
     * @param <U>    the type used to transfer information from originals to shared
     *               replicas on remote places
     * @param ranges the ranges to braodcast
     * @param pack   the function used to transform T objects into the U type used
     *               for transfer
     * @param unpack the closure used to update the T objects based on on the
     *               received U objects
     *
     */
    public <U> void bcast(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack) {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        bcast(ranges, pack, unpack, mm);
        mm.execute();
    }

    /**
     * Conducts a broadcast operation on chunks that are already shared within the
     * place group. The user must call each of the broadcast methods of a cachable
     * chunked list in all the place belonging to the place group.
     *
     * @param <U>    the type used to transfer information from originals to shared
     *               replicas on remote places
     * @param ranges the ranges to braodcast
     * @param pack   the function used to transform T objects into the U type used
     *               for transfer
     * @param unpack the closure used to update the T objects based on on the
     *               received U objects
     * @param mm     the relocator in charge of handling the communication between
     *               hosts
     */
    public <U> void bcast(List<LongRange> ranges, Function<T, U> pack, BiConsumer<T, U> unpack,
            CollectiveRelocator.Allgather mm) {
        final List<RangedList<T>> chunks = searchSharedChunks(here(), ranges);
        final Serializer serProcess = (ObjectOutput s) -> {
            s.writeObject(ranges);
            for (final RangedList<T> chunk : chunks) {
                chunk.forEach((T elem) -> {
                    s.writeObject(pack.apply(elem));
                });
            }
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place p) -> {
            if (p.equals(here())) {
                return;
            }
            @SuppressWarnings("unchecked")
            final List<LongRange> rangesX = (List<LongRange>) ds.readObject();
            final List<RangedList<T>> receiving = searchSharedChunks(p, rangesX);
            for (final RangedList<T> chunk : receiving) {
                chunk.forEach((T elem) -> {
                    @SuppressWarnings("unchecked")
                    final U diff = (U) ds.readObject();
                    unpack.accept(elem, diff);
                });
            }
        };
        mm.request(serProcess, desProcess);
    }

    /**
     * Conducts a broadcast operation on chunks that are already shared within the
     * place group. The user must call each of the broadcast methods of a cachable
     * chunked list in all the place belonging to the place group.
     *
     * @param <U>    the type used to transfer information from originals to shared
     *               replicas on remote places
     * @param range  the range to braodcast
     * @param pack   the function used to transform T objects into the U type used
     *               for transfer
     * @param unpack the closure used to update the T objects based on on the
     *               received U objects
     */
    public <U> void bcast(LongRange range, Function<T, U> pack, BiConsumer<T, U> unpack) {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        bcast(range, pack, unpack, mm);
        mm.execute();
    }

    /**
     * Conducts a broadcast operation on chunks that are already shared within the
     * place group. The user must call each of the broadcast methods of a cachable
     * chunked list in all the place belonging to the place group.
     *
     * @param <U>    the type used to transfer information from originals to shared
     *               replicas on remote places
     * @param range  the range to braodcast
     * @param pack   the function used to transform T objects into the U type used
     *               for transfer
     * @param unpack the closure used to update the T objects based on on the
     *               received U objects
     * @param mm     the relocator in charge of handling the communication between
     *               hosts
     */
    public <U> void bcast(LongRange range, Function<T, U> pack, BiConsumer<T, U> unpack,
            CollectiveRelocator.Allgather mm) {
        bcast(Collections.singletonList(range), pack, unpack, mm);
    }

    @Override
    public void clear() {
        // TODO
        // The super of clear() assumes teamed operation of clear();
    }

    private List<RangedList<T>> exportLocalChunks(List<LongRange> ranges) {
        // TODO
        // Should we check the overlaps in ranges?
        final ArrayList<RangedList<T>> result = new ArrayList<>();
        for (final LongRange range : ranges) {
            forEachChunk(range, (RangedList<T> chunk) -> {
                final LongRange r0 = chunk.getRange();
                if (range.contains(r0)) {
                    addNewShared(here(), chunk);
                    result.add(chunk);
                } else if (r0.from < range.from && r0.to > range.to) {
                    if (attemptSplitChunkAtTwoPoints(range)) {
                        final RangedList<T> c = getChunk(range);
                        addNewShared(here(), c);
                        result.add(c);
                    } else {
                        throw new ConcurrentModificationException();
                    }
                } else {
                    final long splitPoint = (r0.from >= range.from) ? range.to : range.from;
                    final LongRange rRange = (r0.from >= range.from) ? new LongRange(r0.from, range.to)
                            : new LongRange(range.from, r0.to);
                    if (attemptSplitChunkAtSinglePoint(new LongRange(splitPoint))) {
                        final RangedList<T> c = getChunk(rRange);
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

    /**
     * Performs the provided operation on each {@link Chunk}s that are already
     * shared within the place group and overlapped with the given range.
     *
     * @param range range to be scanned
     * @param func  operation to make on each chunk
     */
    public void forEachSharedChunk(LongRange range, Consumer<RangedList<T>> func) {
        shared.forEachChunk(range, func);
    }

    /**
     * Performs the provided operation on each element contained in already shared
     * {@link Chunk} which owner place is here and overlapped with the given range.
     *
     * @param range range to be scanned
     * @param func  operation to make on each element
     */
    public void forEachSharedOwner(LongRange range, Consumer<T> func) {
        shared.forEachChunk(range, (RangedList<T> r0) -> {
            if (shared2owner.get(r0).equals(here())) {
                if (!range.contains(r0.getRange())) {
                    r0 = r0.subList(range);
                }
                r0.forEach(func);
            }
        });
    }

    /**
     * Performs the provided operation on each element contained in already shared
     * {@link Chunk} which owner place is here and overlapped with the given range.
     *
     * @param range range to be scanned
     * @param func  to action to perform on each pair of ({@code long} key and (T)
     *              element
     */
    public void forEachSharedOwner(LongRange range, LongTBiConsumer<T> func) {
        shared.forEachChunk(range, (RangedList<T> r0) -> {
            if (shared2owner.get(r0).equals(here())) {
                if (!range.contains(r0.getRange())) {
                    r0 = r0.subList(range);
                }
                r0.forEach(func);
            }
        });
    }

    /**
     * Returns a newly created snapshot of the current distribution of this
     * collection as a {@link LongRangeDistribution}. This returned distribution's
     * contents will become out-of-date if the contents of this class are relocated,
     * added, and/or removed. <b>The distribution does not include shared
     * ranges.</b>
     * <p>
     * If you need a {@link LongRangeDistribution} to remain up-to-date with the
     * actual distribution of a {@link DistCol}, considers using
     * {@link #registerDistribution(UpdatableDistribution)}. By registering a
     * {@link LongRangeDistribution}, changes in the distribution of entries of this
     * {@link DistCol} will be reflected in the {@link LongRangeDistribution} object
     * when the distribution information of {@link DistCol} is updated and
     * synchronized between hosts using {@link #updateDist()}. This is more
     * efficient than allocating a new {@link LongRangeDistribution} object each
     * time the distribution of the distributed collection changes.
     *
     * @return a new {@link LongRangeDistribution} object representing the current
     *         distribution of this collection
     */
    @Override
    public LongRangeDistribution getDistribution() {
        return super.getDistribution();
    }

    /**
     * Returns a place where a given chunk is owned.
     *
     * @param chunk to find owner place.
     * @return a place a place where a given chunk is owned.
     */
    public Place getSharedOwner(RangedList<T> chunk) {
        return shared2owner.get(chunk);
    }

    @Override
    protected void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManager mm) {
        // check or filter out shared ones.
        for (final RangedList<T> c : cs) {
            assertUnshared(c.getRange());
        }
        super.moveAtSync(cs, dest, mm);
    }

    /**
     * Conduct reduce operation on chunks that are already shared with other places
     * in the given ranges. The reduced result is stored in owner chunks. The user
     * must call each of the reduce methods of a cachable chunked list in all the
     * place belonging to the place group.
     *
     * @param ranges the list of ranges in which chunks are applied to the
     *               operation.
     * @param pack   the function that receives an element and extracts data that
     *               will be transferred to other places and be reduced by the
     *               unpack operation.
     * @param unpack the function that receives a local element and the transferred
     *               data from each place and conducts reduction operation to the
     *               local element.
     * @param <U>    the type of the extracted data
     */
    public <U> void reduce(List<LongRange> ranges, Function<T, U> pack, SerializableBiConsumer<T, U> unpack) {
        final CollectiveMoveManager mm = new CollectiveMoveManager(placeGroup());
        reduce(ranges, pack, unpack, mm);
        try {
            mm.sync();
        } catch (final Exception e) {
            e.printStackTrace();
            throw new Error("Exception raised during CachbleArray#reduce().");
        }
    }

    /**
     * Conduct reduce operation on chunks that are already shared with other places
     * in the given ranges. The reduced result is stored in owner chunks. The user
     * must call each of the reduce methods of a cachable chunked list in all the
     * place belonging to the place group.
     *
     * @param ranges the list of ranges in which chunks are applied to the
     *               operation.
     * @param pack   the function that receives an element and extracts data that
     *               will be transferred to other places and be reduced by the
     *               unpack operation.
     * @param unpack the function that receives a local element and the transferred
     *               data from each place and conducts reduction operation to the
     *               local element.
     * @param mm     You can relocate multiple cachable chunked lists using the same
     *               collective relocator, specified with {@code mm}.
     * @param <U>    the type of the extracted data
     */
    public <U> void reduce(List<LongRange> ranges, final Function<T, U> pack, final SerializableBiConsumer<T, U> unpack,
            CollectiveMoveManager mm) {
        final CachableChunkedList<T> toBranch = this;
        for (final Place p : placeGroup().places()) {
            if (p.equals(here())) {
                continue;
            }
            final List<RangedList<T>> chunks = searchSharedChunks(p, ranges);
            final Serializer serProcess = (ObjectOutput s) -> {
                s.writeInt(chunks.size());
                for (final RangedList<T> chunk : chunks) {
                    s.writeObject(chunk.getRange());
                    chunk.forEach((T elem) -> {
                        s.writeObject(pack.apply(elem));
                    });
                }
            };
            final DeSerializer desProcess = (ObjectInput ds) -> {
                final int n = ds.readInt();
                for (int i = 0; i < n; i++) {
                    final LongRange range0 = (LongRange) ds.readObject();
                    if (!toBranch.containsRange(range0)) {
                        throw new ConcurrentModificationException(
                                "The specified range seems to be remove from " + toBranch + " at " + here());
                    }
                    toBranch.forEach(range0, (T elem) -> {
                        @SuppressWarnings("unchecked")
                        final U diff = (U) ds.readObject();
                        unpack.accept(elem, diff);
                    });
                }
            };
            mm.request(p, serProcess, desProcess);
        }
    }

    @Override
    public RangedList<T> remove(final LongRange r) {
        assertUnshared(r);
        return super.remove(r);
    }

    private List<RangedList<T>> searchSharedChunks(List<LongRange> ranges) {
        final ArrayList<RangedList<T>> result = new ArrayList<>();
        for (final LongRange range : ranges) {
            shared.forEachChunk(range, (RangedList<T> r0) -> {
                if (range.contains(r0.getRange())) {
                    result.add(r0);
                } else {
                    result.add(r0.subList(range));
                }
            });
        }
        return result;
    }

    private List<RangedList<T>> searchSharedChunks(Place owner, List<LongRange> ranges) {
        final ArrayList<RangedList<T>> result = new ArrayList<>();
        for (final LongRange range : ranges) {
            shared.forEachChunk(range, (RangedList<T> r0) -> {
                if (shared2owner.get(r0).equals(owner)) {
                    if (range.contains(r0.getRange())) {
                        result.add(r0);
                    } else {
                        result.add(r0.subList(range));
                    }
                }
            });
        }
        return result;
    }

    @Override
    public void setProxyGenerator(Function<Long, T> func) {
        throw new UnsupportedOperationException("CachableChunkedList does not support proxy feature.");
    }

    /**
     * conduct broadcast operation on chunks that are not shared with other places
     * yet. The user must call each of the share methods of a cachable chunked list
     * in all the place belonging to the place group. This method should not be
     * called simultaneously with other collective methods. The caller place is
     * treated as the owner even if the chunks become shared.
     * <p>
     * Note 1: if you want to share all the local chunks, please call
     * {@link #share()}
     * <p>
     * Note 2: if you want to specify multiple ranges, please use
     * {@link #share(List)}.
     * <p>
     * Note 3: if you don't want to share any local chunks from the called place,
     * please specify an empty range or an empty list of ranges.
     * <p>
     * Note 4: if you want to conduct the relocation process of multiple cachable
     * chunked lists using the same ObjectOutput(Stream), please prepare an instance
     * of {@link CollectiveRelocator.Allgather} first and call the relocation
     * methods of the cachable chunked lists in the same order specifying the
     * collective relocator as a parameter, and finally call the execute method of
     * the relocator.
     */
    public void share() {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        share(Collections.singletonList(null), mm);
        mm.execute();
    }

    /**
     * conduct broadcast operation on chunks that are not shared with other places
     * yet. The user must call each of the share methods of a cachable chunked list
     * in all the place belonging to the place group. This method should not be
     * called simultaneously with other collective methods. The caller place is
     * treated as the owner even if the chunks become shared.
     * <p>
     * Note 1: if you want to share all the local chunks, please call
     * {@link #share()}
     * <p>
     * Note 2: if you want to specify multiple ranges, please use
     * {@link #share(List)}.
     * <p>
     * Note 3: if you don't want to share any local chunks from the called place,
     * please specify an empty range or an empty list of ranges.
     * <p>
     * Note 4: if you want to conduct the relocation process of multiple cachable
     * chunked lists using the same ObjectOutput(Stream), please prepare an instance
     * of {@link CollectiveRelocator.Allgather} first and call the relocation
     * methods of the cachable chunked lists in the same order specifying the
     * collective relocator as a parameter, and finally call the execute method of
     * the relocator.
     *
     * @param mm You can relocate multiple cachable chunked lists using the same
     *           collective relocator, specified with {@code mm}.
     */
    public void share(CollectiveRelocator.Allgather mm) {
        share(Collections.singletonList(null), mm);
    }

    /**
     * conduct broadcast operation on chunks that are not shared with other places
     * yet. The user must call each of the share methods of a cachable chunked list
     * in all the place belonging to the place group. This method should not be
     * called simultaneously with other collective methods. The caller place is
     * treated as the owner even if the chunks become shared.
     * <p>
     * Note 1: if you want to share all the local chunks, please call
     * {@link #share()}
     * <p>
     * Note 2: if you want to specify multiple ranges, please use
     * {@link #share(List)}.
     * <p>
     * Note 3: if you don't want to share any local chunks from the called place,
     * please specify an empty range or an empty list of ranges.
     * <p>
     * Note 4: if you want to conduct the relocation process of multiple cachable
     * chunked lists using the same ObjectOutput(Stream), please prepare an instance
     * of {@link CollectiveRelocator.Allgather} first and call the relocation
     * methods of the cachable chunked lists in the same order specifying the
     * collective relocator as a parameter, and finally call the execute method of
     * the relocator.
     *
     * @param ranges The library scans the ranges and exports (the parts of) the
     *               local chunks in the ranges.
     */
    public void share(List<LongRange> ranges) {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        share(ranges, mm);
        mm.execute();
    }

    /**
     * conduct broadcast operation on chunks that are not shared with other places
     * yet. The user must call each of the share methods of a cachable chunked list
     * in all the place belonging to the place group. This method should not be
     * called simultaneously with other collective methods. The caller place is
     * treated as the owner even if the chunks become shared.
     * <p>
     * Note 1: if you want to share all the local chunks, please call
     * {@link #share()}
     * <p>
     * Note 2: if you want to specify multiple ranges, please use
     * {@link #share(List)}.
     * <p>
     * Note 3: if you don't want to share any local chunks from the called place,
     * please specify an empty range or an empty list of ranges.
     * <p>
     * Note 4: if you want to conduct the relocation process of multiple cachable
     * chunked lists using the same ObjectOutput(Stream), please prepare an instance
     * of {@link CollectiveRelocator.Allgather} first and call the relocation
     * methods of the cachable chunked lists in the same order specifying the
     * collective relocator as a parameter, and finally call the execute method of
     * the relocator.
     *
     * @param ranges The library scans the ranges and exports (the parts of) the
     *               local chunks in the ranges.
     * @param mm     You can relocate multiple cachable chunked lists using the same
     *               collective relocator, specified with {@code mm}.
     */
    public void share(final List<LongRange> ranges, CollectiveRelocator.Allgather mm) {
        final List<RangedList<T>> chunks = exportLocalChunks(ranges);
        final Serializer serProcess = (ObjectOutput s) -> {
            s.writeObject(chunks);
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place sender) -> {
            @SuppressWarnings("unchecked")
            final List<RangedList<T>> received = (List<RangedList<T>>) ds.readObject();
            addNewShared(sender, received);
        };
        mm.request(serProcess, desProcess);
    }

    /**
     * conduct broadcast operation on chunks that are not shared with other places
     * yet. The user must call each of the share methods of a cachable chunked list
     * in all the place belonging to the place group. This method should not be
     * called simultaneously with other collective methods. The caller place is
     * treated as the owner even if the chunks become shared.
     * <p>
     * Note 1: if you want to share all the local chunks, please call
     * {@link #share()}
     * <p>
     * Note 2: if you want to specify multiple ranges, please use
     * {@link #share(List)}.
     * <p>
     * Note 3: if you don't want to share any local chunks from the called place,
     * please specify an empty range or an empty list of ranges.
     * <p>
     * Note 4: if you want to conduct the relocation process of multiple cachable
     * chunked lists using the same ObjectOutput(Stream), please prepare an instance
     * of {@link CollectiveRelocator.Allgather} first and call the relocation
     * methods of the cachable chunked lists in the same order specifying the
     * collective relocator as a parameter, and finally call the execute method of
     * the relocator.
     *
     * @param range The library scans the range and exports (the parts of) the local
     *              chunks in the range.
     */
    public void share(LongRange range) {
        final CollectiveRelocator.Allgather mm = new CollectiveRelocator.Allgather(placeGroup());
        share(Collections.singletonList(range), mm);
        mm.execute();
    }

    /**
     * conduct broadcast operation on chunks that are not shared with other places
     * yet. The user must call each of the share methods of a cachable chunked list
     * in all the place belonging to the place group. This method should not be
     * called simultaneously with other collective methods. The caller place is
     * treated as the owner even if the chunks become shared.
     * <p>
     * Note 1: if you want to share all the local chunks, please call
     * {@link #share()}
     * <p>
     * Note 2: if you want to specify multiple ranges, please use
     * {@link #share(List)}.
     * <p>
     * Note 3: if you don't want to share any local chunks from the called place,
     * please specify an empty range or an empty list of ranges.
     * <p>
     * Note 4: if you want to conduct the relocation process of multiple cachable
     * chunked lists using the same ObjectOutput(Stream), please prepare an instance
     * of {@link CollectiveRelocator.Allgather} first and call the relocation
     * methods of the cachable chunked lists in the same order specifying the
     * collective relocator as a parameter, and finally call the execute method of
     * the relocator.
     *
     * @param range The library scans the range and exports (the parts of) the local
     *              chunks in the range.
     * @param mm    You can relocate multiple cachable chunked lists using the same
     *              collective relocator, specified with {@code mm}.
     */
    public void share(LongRange range, CollectiveRelocator.Allgather mm) {
        share(Collections.singletonList(range), mm);
    }

    /**
     * Returns ChunkedList contains chunks that are already shared within the place
     * group. The returned shared chunkedList has an unmodifiable structure.
     * Operations such as add and remove cannot be performed.
     *
     * @return ChunkedList contains chunks that are already shared
     */
    public ChunkedList<T> sharedChunks() {
        return new ChunkedList.UnmodifiableView<>(shared);
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = manager.placeGroup;
        final GlobalID id1 = id();
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new CachableChunkedList<>(pg1, id1);
        });
    }

    // TODO
    // prepare documents for the following methods
    // getSizedistribution: only returns unshared
    // getDistribution: only returns unshared
    // getRangedDistribution: only returns unshared
    // TEAM: only support DistCol methods

}
