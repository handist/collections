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

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.ChunkedList;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.dist.util.Pair;
import handist.collections.function.DeSerializer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.Serializer;
import handist.collections.glb.DistColGlb;

/**
 * A class for handling objects at multiple places. It is allowed to add new
 * elements dynamically. This class provides the method for load balancing.
 * <p>
 * Note: In the current implementation, there are some limitations.
 * <ul>
 * <li>There is only one load balancing method: the method flattens the number
 * of elements of the all places.
 * </ul>
 *
 * @param <T> the type of elements handled by this {@link DistChunkedList}
 */
@DefaultSerializer(JavaSerializer.class)
public class DistChunkedList<T> extends ChunkedList<T>
        implements DistributedCollection<T, DistChunkedList<T>>, RangeRelocatable<LongRange>, SerializableWithReplace {

    static class DistributionManager<T> extends GeneralDistManager<DistChunkedList<T>> implements Serializable {

        /** Serial Version UID */
        private static final long serialVersionUID = 456677767130722164L;

        public DistributionManager(TeamedPlaceGroup placeGroup, GlobalID id, DistChunkedList<T> branch) {
            super(placeGroup, id, branch);
        }

        @Override
        protected void moveAtSyncCount(ArrayList<IntLongPair> moveList, CollectiveMoveManager mm) throws Exception {
            branch.moveAtSyncCount(moveList, mm);
        }

    }

    /**
     * Specialization of {@link TeamOperations} for the purposes of
     * {@link DistChunkedList}
     *
     * @param <T> the type of the instances held by the underlying
     *            {@link DistChunkedList}
     */
    public static class Team<T> extends TeamOperations<T, DistChunkedList<T>> {

        private Team(DistChunkedList<T> localObject) {
            super(localObject);
        }

        /**
         * Performs a parallel reduction on each local handle of the underlyin
         * {@link DistChunkedList} collection before reducing the result of each
         * individual host. This method is blocking and needs to be called on all hosts
         * to terminate.
         *
         * @param <R>     the type of the reducer used
         * @param reducer the reduction operation to perform
         * @return the result of the specified reduction across all local handles of the
         *         underlying collection
         */
        public <R extends Reducer<R, T>> R parallelReduce(R reducer) {
            final R localReduce = handle.parallelReduce(reducer);
            return localReduce.teamReduction(handle.placeGroup());
        }

        /**
         * Performs a sequential reduction on each handle of the underlying
         * {@link DistChunkedList} collection before reducing the result of each
         * individual host. This method is blocking and needs to be called on all hosts
         * to terminate.
         *
         * @param <R>     type of the reducer used
         * @param reducer the reduction operation to perform
         * @return the result of the specified reduction across all local handles of the
         *         underlying collection
         */
        public <R extends Reducer<R, T>> R reduce(R reducer) {
            final R localReduce = handle.reduce(reducer);

            return localReduce.teamReduction(handle.placeGroup());
        }
    }

    private static int _debug_level = 5;

    private static float[] initialLocality(final int size) {
        final float[] result = new float[size];
        Arrays.fill(result, 1.0f);
        return result;
    }

    /**
     * Handle to operations that can benefit from load balance when called inside an
     * "underGLB" method.
     */
    public transient final DistColGlb<T> GLB;

    /**
     * Handle to Global Operations implemented by {@link DistChunkedList}.
     */
    public transient final GlobalOperations<T, DistChunkedList<T>> GLOBAL;

    protected transient DistributionManager<T> manager;

    /**
     * Handle to Team Operations implemented by {@link DistChunkedList}.
     */
    protected final transient Team<T> TEAM;

    @SuppressWarnings("rawtypes")
    DistCollectionSatellite satellite;

    /**
     * Create a new DistCol. All the hosts participating in the distributed
     * computation are susceptible to handle the created instance. This constructor
     * is equivalent to calling {@link #DistChunkedList(TeamedPlaceGroup)} with
     * {@link TeamedPlaceGroup#getWorld()} as argument.
     */
    public DistChunkedList() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Constructor for {@link DistChunkedList} whose handles are restricted to the
     * {@link TeamedPlaceGroup} passed as parameter.
     *
     * @param placeGroup the places on which the DistCol will hold handles.
     */
    public DistChunkedList(final TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Private constructor by which the locality and the GlobalId associated with
     * the {@link DistChunkedList} are explicitly given. This constructor should
     * only be used internally when creating the local handle of a DistCol already
     * created on a remote place. Calling this constructor with an existing
     * {@link GlobalID} which is already linked with existing and potentially
     * different objects could prove disastrous.
     *
     * @param placeGroup the hosts on which the distributed collection the created
     *                   instance may have handles on
     * @param id         the global id used to identify all the local handles
     */
    private DistChunkedList(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        this(placeGroup, id, (TeamedPlaceGroup pg, GlobalID gid) -> new DistChunkedList<>(pg, gid));
    }

    protected DistChunkedList(final TeamedPlaceGroup placeGroup, final GlobalID id,
            BiFunction<TeamedPlaceGroup, GlobalID, ? extends DistChunkedList<T>> lazyCreator) {
        super();
        id.putHere(this);
        manager = new DistributionManager<>(placeGroup, id, this);
        manager.locality = initialLocality(placeGroup.size);
        TEAM = new Team<>(this);
        GLOBAL = new GlobalOperations<>(this, lazyCreator);
        GLB = new DistColGlb<>(this);
    }

    @Override
    public void clear() {
        super.clear();
        Arrays.fill(manager.locality, 1.0f);
    }

    @Override
    public void forEach(SerializableConsumer<T> action) {
        super.forEach(action);
    }

    @Override
    public Collection<LongRange> getAllRanges() {
        return ranges();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <S extends DistCollectionSatellite<DistChunkedList<T>, S>> S getSatellite() {
        return (S) satellite;
    }

    @Override
    public GlobalOperations<T, DistChunkedList<T>> global() {
        return GLOBAL;
    }

    @Override
    public int hashCode() {
        return (int) id().gid();
    }

    @Override
    public GlobalID id() {
        return manager.id;
    }

    @Override
    public float[] locality() {
        // TODO check if this is correct
        return manager.locality;
    }

    @Override
    public long longSize() {
        return super.size();
    }

    @SuppressWarnings("unchecked")
    protected void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManager mm) {
        if (_debug_level > 5) {
            System.out.print("[" + here().id + "] moveAtSync List[RangedList[T]]: ");
            for (final RangedList<T> rl : cs) {
                System.out.print("" + rl.getRange() + ", ");
            }
            System.out.println(" dest: " + dest.id);
        }

        if (dest.equals(here())) {
            return;
        }

        final DistChunkedList<T> toBranch = this; // using plh@AbstractCol
        final Serializer serialize = (ObjectOutput s) -> {
            for (final RangedList<T> c : cs) {
                this.remove(c.getRange());
            }
            s.writeObject(cs);
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final List<RangedList<T>> chunks = (List<RangedList<T>>) ds.readObject();
            for (final RangedList<T> c : chunks) {
                toBranch.add(c);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    @Override
    public void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManager mm) throws Exception {
        // TODO ->LinkedList? sort??
        final ArrayList<LongRange> localKeys = new ArrayList<>();
        localKeys.addAll(ranges());
        localKeys.sort((LongRange range1, LongRange range2) -> {
            final long len1 = range1.to - range1.from;
            final long len2 = range2.to - range2.from;
            return (int) (len1 - len2);
        });
        if (_debug_level > 5) {
            System.out.print("[" + here() + "] ");
            for (int i = 0; i < localKeys.size(); i++) {
                System.out.print("" + localKeys.get(i).from + ".." + localKeys.get(i).to + ", ");
            }
            System.out.println();
        }
        for (final IntLongPair moveinfo : moveList) {
            final long count = moveinfo.second;
            final Place dest = manager.placeGroup.get(moveinfo.first);
            if (_debug_level > 5) {
                System.out.println("[" + here() + "] move count=" + count + " to dest " + dest.id);
            }
            if (dest.equals(here())) {
                continue;
            }
            long sizeToSend = count;
            while (sizeToSend > 0) {
                final LongRange lk = localKeys.remove(0);
                final long len = lk.to - lk.from;
                if (len > sizeToSend) {
                    moveRangeAtSync(new LongRange(lk.from, lk.from + sizeToSend), dest, mm);
                    localKeys.add(0, new LongRange(lk.from + sizeToSend, lk.to));
                    break;
                } else {
                    moveRangeAtSync(lk, dest, mm);
                    sizeToSend -= len;
                }
            }
        }
    }

    public void moveRangeAtSync(Function<LongRange, List<Pair<Place, LongRange>>> rule, CollectiveMoveManager mm) {
        final DistChunkedList<T> collection = this;
        final HashMap<Place, ArrayList<LongRange>> rangesToMove = new HashMap<>();

        collection.forEachChunk((RangedList<T> c) -> {
            final List<Pair<Place, LongRange>> destinationList = rule.apply(c.getRange());
            for (final Pair<Place, LongRange> destination : destinationList) {
                final Place destinationPlace = destination.first;
                final LongRange destinationRange = destination.second;
                if (!rangesToMove.containsKey(destinationPlace)) {
                    rangesToMove.put(destinationPlace, new ArrayList<LongRange>());

                }
                rangesToMove.get(destinationPlace).add(destinationRange);
            }
        });
        for (final Place place : rangesToMove.keySet()) {
            for (final LongRange range : rangesToMove.get(place)) {
                moveRangeAtSync(range, place, mm);
            }
        }
    }

    /**
     * This implementation only accepts for range that either match an existing
     * chunk contained in this collection or a range which is entirely contained
     * within a single chunk of this collection. Ranges which span multiple chunks
     * will cause exceptions to be thrown.
     */
    @Override
    public void moveRangeAtSync(final LongRange range, final Place dest, final MoveManager mm) {
        if (_debug_level > 5) {
            System.out.println("[" + here().id + "] moveAtSync range: " + range + " dest: " + dest.id);
        }

        final ArrayList<RangedList<T>> chunksToMove = splitChunks(range);
        if (chunksToMove.isEmpty()) {
            return;
        }
        moveAtSync(chunksToMove, dest, mm);
    }

    public void moveRangeAtSync(final RangedDistribution<LongRange> rangedDistribution, final CollectiveMoveManager mm)
            throws Exception {
        for (final LongRange r : ranges()) {
            final Map<LongRange, Place> relocation = rangedDistribution.rangeLocation(r);
            for (final Map.Entry<LongRange, Place> reloc : relocation.entrySet()) {
                moveRangeAtSync(reloc.getKey(), reloc.getValue(), mm);
            }
        }
    }

    // Method moved to GLOBAL and TEAM operations
    // @Override
    // public void distSize(long[] result) {
    // for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
    // final LongRange k = entry.getKey();
    // final Place p = entry.getValue();
    // result[manager.placeGroup.rank(p)] += k.size();
    // }
    // }

    @Override
    public void parallelForEach(SerializableConsumer<T> action) {
        super.parallelForEach(action);
    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        return manager.placeGroup;
    }

    @Override
    public <S extends DistCollectionSatellite<DistChunkedList<T>, S>> void setSatellite(S s) {
        satellite = s;
    }

    @Override
    public Team<T> team() {
        return TEAM;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + ":" + id();
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = manager.placeGroup;
        final GlobalID id1 = id();
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistChunkedList<>(pg1, id1);
        });
    }

}
