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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;

import apgas.Place;
import apgas.util.GlobalID;
import apgas.util.SerializableWithReplace;
import handist.collections.ChunkedList;
import handist.collections.ElementOverlapException;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.util.IntLongPair;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.MemberOfLazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.dist.util.Pair;
import handist.collections.function.DeSerializer;
import handist.collections.function.SerializableBiConsumer;
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
 * @param <T> the type of elements handled by this {@link DistCol}
 */
@DefaultSerializer(JavaSerializer.class)
public class DistCol<T> extends ChunkedList<T>
        implements DistributedCollection<T, DistCol<T>>, RangeRelocatable<LongRange>, SerializableWithReplace {

    /**
     * Class used to identify pieces of chunks when a chunk is split in two and each
     * instance is transferred to another place. This class implements a sort of
     * view of the separated pieces.
     *
     * @param <T> type of the individual element contained in the chunk
     * @see ChunkExtractMiddle
     * @see ChunkExtractRight
     */
    static class ChunkExtractLeft<T> {
        /** Original RangedList that was split */
        public RangedList<T> original;
        /** index at which the ranged list was split */
        public long splitPoint;

        /**
         * Constructor. The original ranged list and the index at which it was split are
         * specified as parameters
         *
         * @param original   ranged list which is being split
         * @param splitPoint index at which the ranged list is split
         */
        ChunkExtractLeft(final RangedList<T> original, final long splitPoint) {
            this.original = original;
            this.splitPoint = splitPoint;
        }

        /**
         * Obtain the various RangedList that are now being handled as a result of
         * separating a Ranged list at the index specified by this instance
         *
         * @return the multiple ranged list that result of the original being split at
         *         the index specified
         */
        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint);
        }
    }

    /**
     * Class used to identify pieces of chunks when a chunk is split in two and each
     * instance is transferred to another place. This class implements a sort of
     * view of the separated pieces.
     *
     * @param <T> type of the individual element contained in the chunk
     * @see ChunkExtractLeft
     * @see ChunkExtractRight
     */
    static class ChunkExtractMiddle<T> {
        /** Original RangedList whose piece is being considered for splitting */
        public RangedList<T> original;
        /** First index at which the ranged list is being split */
        public long splitPoint1;
        /** First index at which the ranged list is being split */
        public long splitPoint2;

        /**
         * Constructor. The original ranged list and the index at which it was split are
         * specified as parameters
         *
         * @param original    ranged list which is being split
         * @param splitPoint1 first index at which the ranged list is split
         * @param splitPoint2 second index at which the ranged list is split
         */
        ChunkExtractMiddle(final RangedList<T> original, final long splitPoint1, final long splitPoint2) {
            this.original = original;
            this.splitPoint1 = splitPoint1;
            this.splitPoint2 = splitPoint2;
        }

        /**
         * Obtain the various RangedList that are now being handled as a result of
         * separating a Ranged list at the index specified by this instance
         *
         * @return the multiple ranged list that result of the original being split at
         *         the indices specified
         */
        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint1, splitPoint2);
        }
    }

    /**
     * Class used to identify pieces of chunks when a chunk is split in two and each
     * instance is transferred to another place. This class implements a sort of
     * view of the separated pieces.
     *
     * @param <T> type of the individual element contained in the chunk
     * @see ChunkExtractLeft
     * @see ChunkExtractMiddle
     */
    static class ChunkExtractRight<T> {
        /** Original ranged list considered for splitting */
        public RangedList<T> original;
        /** Index at which the ranged list is being split */
        public long splitPoint;

        /**
         * Constructor. The original ranged list and the index at which it was split are
         * specified as parameters
         *
         * @param original   ranged list which is being split
         * @param splitPoint index at which the ranged list is split
         */
        ChunkExtractRight(final RangedList<T> original, final long splitPoint) {
            this.original = original;
            this.splitPoint = splitPoint;
        }

        /**
         * Obtain the various RangedList that are now being handled as a result of
         * separating a Ranged list at the index specified by this instance
         *
         * @return the multiple ranged list that result of the original being split at
         *         the index specified
         */
        List<RangedList<T>> extract() {
            return original.splitRange(splitPoint);
        }
    }

    /**
     * Global handle for the {@link DistCol} class. This class make operations
     * operating on all the local handles of the distributed collections accessible.
     *
     * @author Patrick Finnerty
     *
     */
    public class DistColGlobal extends GlobalOperations<T, DistCol<T>> implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = 4584810477237588857L;

        /**
         * Constructor
         *
         * @param handle handle to the local DistCol instance
         */
        public DistColGlobal(DistCol<T> handle) {
            super(handle);
        }

        @Override
        public void onLocalHandleDo(SerializableBiConsumer<Place, DistCol<T>> action) {
            localHandle.placeGroup().broadcastFlat(() -> {
                action.accept(here(), localHandle);
            });
        }

        @Override
        public Object writeReplace() throws ObjectStreamException {
            final TeamedPlaceGroup pg1 = localHandle.placeGroup();
            final GlobalID id1 = localHandle.id();
            return new MemberOfLazyObjectReference<>(pg1, id1, () -> {
                return new DistCol<>(pg1, id1);
            }, (instanceOfDistCol) -> {
                return instanceOfDistCol.GLOBAL;
            });
        }

    }

    /**
     * TEAM handle for methods that can be called concurrently to other methods on
     * all local handles of {@link DistCol}
     *
     * @author Patrick Finnerty
     *
     */
    public class DistColTeam extends TeamOperations<T, DistCol<T>> implements Serializable {

        /** Serial Version UID */
        private static final long serialVersionUID = -5392694230295904290L;

        /**
         * Constructor
         *
         * @param handle local handle of {@link DistCol} that this instance is managing
         */
        DistColTeam(DistCol<T> handle) {
            super(handle);
        }

        @Override
        public void size(long[] result) {
            for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
                final LongRange k = entry.getKey();
                final Place p = entry.getValue();
                result[manager.placeGroup.rank(p)] += k.size();
            }
        }

        @Override
        public void updateDist() {
            team_updateDist();
        }
    }

    static class DistributionManager<T> extends GeneralDistManager<DistCol<T>> implements Serializable {

        /** Serial Version UID */
        private static final long serialVersionUID = 456677767130722164L;

        public DistributionManager(TeamedPlaceGroup placeGroup, GlobalID id, DistCol<T> branch) {
            super(placeGroup, id, branch);
        }

        @Override
        public void checkDistInfo(long[] result) {
            for (final Map.Entry<LongRange, Place> entry : branch.ldist.dist.entrySet()) {
                final LongRange k = entry.getKey();
                final Place p = entry.getValue();
                result[placeGroup.rank(p)] += k.size();
            }
        }

        @Override
        protected void moveAtSyncCount(ArrayList<IntLongPair> moveList, CollectiveMoveManager mm) throws Exception {
            branch.moveAtSyncCount(moveList, mm);
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
     * Handle to Global Operations implemented by {@link DistCol}.
     */
    public transient final DistColGlobal GLOBAL;

    /**
     * Internal class that handles distribution-related operations.
     */
    protected final transient DistManager<LongRange> ldist;

    protected transient DistributionManager<T> manager;

    /**
     * Function kept and used when the local handle does not contain the specified
     * index in method {@link #get(long)}. This proxy will return a value to be
     * returned by the {@link #get(long)} method rather than throwing an
     * {@link Exception}.
     */
    private Function<Long, T> proxyGenerator;

    /**
     * Handle to Team Operations implemented by {@link DistCol}.
     */
    public final transient DistColTeam TEAM;

    /**
     * Create a new DistCol. All the hosts participating in the distributed
     * computation are susceptible to handle the created instance. This constructor
     * is equivalent to calling {@link #DistCol(TeamedPlaceGroup)} with
     * {@link TeamedPlaceGroup#getWorld()} as argument.
     */
    public DistCol() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Constructor for {@link DistCol} whose handles are restricted to the
     * {@link TeamedPlaceGroup} passed as parameter.
     *
     * @param placeGroup the places on which the DistCol will hold handles.
     */
    public DistCol(final TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Private constructor by which the locality and the GlobalId associated with
     * the {@link DistCol} are explicitly given. This constructor should only be
     * used internally when creating the local handle of a DistCol already created
     * on a remote place. Calling this constructor with an existing {@link GlobalID}
     * which is already linked with existing and potentially different objects could
     * prove disastrous.
     *
     * @param placeGroup the hosts on which the distributed collection the created
     *                   instance may have handles on
     * @param id         the global id used to identify all the local handles
     */
    private DistCol(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        super();
        id.putHere(this);
        manager = new DistributionManager<>(placeGroup, id, this);
        manager.locality = initialLocality(placeGroup.size);
        ldist = new DistManager<>();
        TEAM = new DistColTeam(this);
        GLOBAL = new DistColGlobal(this);
        GLB = new DistColGlb<>(this);
    }

    @Override
    public void add(final RangedList<T> c) throws ElementOverlapException {
        ldist.add(c.getRange());
        super.add(c);
    }

    @Override
    public void clear() {
        super.clear();
        ldist.clear();
        Arrays.fill(manager.locality, 1.0f);
    }

    @Override
    public void forEach(SerializableConsumer<T> action) {
        super.forEach(action);
    }

    /**
     * Return the value corresponding to the specified index.
     *
     * If the specified index is not located on this place, a
     * {@link IndexOutOfBoundsException} will be raised, except if a proxy generator
     * was set for this instance, in which case the value generated by the proxy is
     * returned.
     *
     * @param index index whose value needs to be retrieved
     * @throws IndexOutOfBoundsException if the specified index is not contained in
     *                                   this local collection and no proxy was
     *                                   defined
     * @return the value corresponding to the provided index, or the value generated
     *         by the proxy if it was defined and the specified index is outside the
     *         range of indices of this local instance
     * @see #setProxyGenerator(Function)
     */
    @Override
    public T get(long index) {
        if (proxyGenerator == null) {
            return super.get(index);
        } else {
            try {
                return super.get(index);
            } catch (final IndexOutOfBoundsException e) {
                return proxyGenerator.apply(index);
            }
        }
    }

    @Override
    public Collection<LongRange> getAllRanges() {
        return ranges();
    }

    Map<LongRange, Integer> getDiff() {
        return ldist.diff;
    }

    public ConcurrentHashMap<LongRange, Place> getDist() {
        return ldist.dist;
    }

    public LongDistribution getDistributionLong() {
        return LongDistribution.convert(getDist());
    }

    public LongRangeDistribution getRangedDistributionLong() {
        return new LongRangeDistribution(getDist());
    }

    @Override
    public GlobalOperations<T, DistCol<T>> global() {
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

    @Deprecated
    @SuppressWarnings("unchecked")
    private void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManager mm) {
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

        final DistCol<T> toBranch = this; // using plh@AbstractCol
        final Serializer serialize = (ObjectOutput s) -> {
            final ArrayList<Byte> keyTypeList = new ArrayList<>();
            for (final RangedList<T> c : cs) {
                keyTypeList.add(ldist.moveOut(c.getRange(), dest));
                this.removeForMove(c.getRange());
            }
            s.writeObject(keyTypeList);
            s.writeObject(cs);
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final List<Byte> keyTypeList = (List<Byte>) ds.readObject();
            final Iterator<Byte> keyTypeListIt = keyTypeList.iterator();
            final List<RangedList<T>> chunks = (List<RangedList<T>>) ds.readObject();
            for (final RangedList<T> c : chunks) {
                final byte keyType = keyTypeListIt.next();
                final LongRange key = c.getRange();
                if (_debug_level > 5) {
                    System.out.println("[" + here() + "] putForMove key: " + key + " keyType: " + keyType);
                }
                toBranch.putForMove(c, keyType);
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

    public void moveRangeAtSync(final Distribution<Long> dist, final CollectiveMoveManager mm) {
        moveRangeAtSync((LongRange range) -> {
            final ArrayList<Pair<Place, LongRange>> listPlaceRange = new ArrayList<>();
            for (final Long key : range) {
                listPlaceRange.add(new Pair<>(dist.place(key), new LongRange(key, key + 1)));
            }
            return listPlaceRange;
        }, mm);
    }

    public void moveRangeAtSync(Function<LongRange, List<Pair<Place, LongRange>>> rule, CollectiveMoveManager mm) {
        final DistCol<T> collection = this;
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

    @Override
    public void moveRangeAtSync(final LongRange range, final Place dest, final MoveManager mm) {
        if (_debug_level > 5) {
            System.out.println("[" + here().id + "] moveAtSync range: " + range + " dest: " + dest.id);
        }
        final ArrayList<RangedList<T>> chunksToMove = new ArrayList<>();
        final ArrayList<ChunkExtractLeft<T>> chunksToExtractLeft = new ArrayList<>();
        final ArrayList<ChunkExtractMiddle<T>> chunksToExtractMiddle = new ArrayList<>();
        final ArrayList<ChunkExtractRight<T>> chunksToExtractRight = new ArrayList<>();
        forEachChunk((RangedList<T> c) -> {
            final LongRange cRange = c.getRange();
            if (cRange.from <= range.from) {
                if (cRange.to <= range.from) { // cRange.max < range.min) {
                    // skip
                } else {
                    // range.min <= cRange.max
                    if (cRange.from == range.from) {
                        if (cRange.to <= range.to) {
                            // add cRange.min..cRange.max
                            chunksToMove.add(c);
                        } else {
                            // range.max < cRange.max
                            // split at range.max/range.max+1
                            // add cRange.min..range.max
                            chunksToExtractLeft.add(new ChunkExtractLeft<>(c, range.to/* max + 1 */));
                        }
                    } else {
                        // cRange.min < range.min
                        if (range.to < cRange.to) {
                            // split at range.min-1/range.min
                            // split at range.max/range.max+1
                            // add range.min..range.max
                            chunksToExtractMiddle.add(new ChunkExtractMiddle<>(c, range.from, range.to/* max + 1 */));
                        } else {
                            // split at range.min-1/range.min
                            // cRange.max =< range.max
                            // add range.min..cRange.max
                            chunksToExtractRight.add(new ChunkExtractRight<>(c, range.from));
                        }
                    }
                }
            } else {
                // range.min < cRange.min
                if (range.to <= cRange.from) { // range.max < cRange.min) {
                    // skip
                } else {
                    // cRange.min <= range.max
                    if (cRange.to <= range.to) {
                        // add cRange.min..cRange.max
                        chunksToMove.add(c);
                    } else {
                        // split at range.max/range.max+1
                        // add cRange.min..range.max
                        chunksToExtractLeft.add(new ChunkExtractLeft<>(c, range.to/* max + 1 */));
                    }
                }
            }
        });

        for (final ChunkExtractLeft<T> chunkToExtractLeft : chunksToExtractLeft) {
            final RangedList<T> original = chunkToExtractLeft.original;
            final List<RangedList<T>> splits = chunkToExtractLeft.extract();
            // System.out.println("[" + here.id + "] removeChunk " + original.getRange());
            remove(original);
            // System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
            add(splits.get(0)/* first */);
            // System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
            add(splits.get(1)/* second */);
            chunksToMove.add(splits.get(0)/* first */);
        }

        for (final ChunkExtractMiddle<T> chunkToExtractMiddle : chunksToExtractMiddle) {
            final RangedList<T> original = chunkToExtractMiddle.original;
            final List<RangedList<T>> splits = chunkToExtractMiddle.extract();
            // System.out.println("[" + here.id + "] removeChunk " + original.getRange());
            remove(original);
            // System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
            add(splits.get(0)/* first */);
            // System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
            add(splits.get(1)/* second */);
            // System.out.println("[" + here.id + "] putChunk " + splits.third.getRange());
            add(splits.get(2)/* third */);
            chunksToMove.add(splits.get(1)/* second */);
        }

        for (final ChunkExtractRight<T> chunkToExtractRight : chunksToExtractRight) {
            final RangedList<T> original = chunkToExtractRight.original;
            final List<RangedList<T>> splits = chunkToExtractRight.extract();
            // System.out.println("[" + here.id + "] removeChunk " + original.getRange());
            remove(original);
            // System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
            add(splits.get(0)/* first */);
            // System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
            add(splits.get(1)/* second */);
            chunksToMove.add(splits.get(1)/* second */);
        }

        moveAtSync(chunksToMove, dest, mm);
    }

    public void moveRangeAtSync(final RangedDistribution<LongRange> dist, final CollectiveMoveManager mm)
            throws Exception {
        moveRangeAtSync((LongRange range) -> {
            return dist.placeRanges(range);
        }, mm);
    }

    @Override
    public void parallelForEach(SerializableConsumer<T> action) {
        super.parallelForEach(action);
    }

    @Override
    public TeamedPlaceGroup placeGroup() {
        return manager.placeGroup;
    }

    private void putForMove(final RangedList<T> c, final byte mType) throws Exception {
        final LongRange key = c.getRange();
        switch (mType) {
        case DistManager.MOVE_NEW:
            ldist.moveInNew(key);
            break;
        case DistManager.MOVE_OLD:
            ldist.moveInOld(key);
            break;
        default:
            throw new Exception("SystemError when calling putForMove " + key);
        }
        super.add(c);
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
    public RangedList<T> remove(final LongRange r) {
        ldist.remove(r);
        return super.remove(r);
    }

    @Deprecated
    @Override
    public RangedList<T> remove(final RangedList<T> c) {
        ldist.remove(c.getRange());
        return super.remove(c);
    }

    private void removeForMove(final LongRange r) {
        if (super.remove(r) == null) {
            throw new RuntimeException("DistCol#removeForMove");
        }
    }

    /**
     * Sets this instance's proxy generator.
     *
     * The proxy feature is used to prepare an element when access to an index that
     * is not contained in the local range. Instead of throwing an
     * {@link IndexOutOfBoundsException}, the value generated by the proxy will be
     * used. It resembles `getOrDefault(key, defaultValue)`.
     *
     * @param proxyGenerator function that takes a {@link Long} index as parameter
     *                       and returns a T
     */
    public void setProxyGenerator(Function<Long, T> proxyGenerator) {
        this.proxyGenerator = proxyGenerator;
    }

    @Override
    public TeamOperations<T, DistCol<T>> team() {
        return TEAM;
    }

    // TODO make private
    private void team_updateDist() {
        ldist.updateDist(manager.placeGroup);
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
            return new DistCol<>(pg1, id1);
        });
    }
}
