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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
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
    public void add_unchecked(RangedList<T> c) {
        ldist.add(c.getRange());
        super.add_unchecked(c);
    }

    /**
     * Method used in preparation before transferring chunks. This method checks if
     * a chunk contained in this object has its range exactly matching the range
     * specified as parameter. If that is the case, returns {@code true}.
     * <p>
     * If that is not the case, i.e. a chunk held by this collection needs to be
     * split so that the specified range can be sent to a remote host, attempts to
     * make the split. If it is successful in splitting the existing chunk so that
     * the specified range has a corresponding chunk stored in this collection,
     * returns {@code true}. If splitting the existing range failed (due to a
     * concurrent attempts to split that range), returns {@code false}. The caller
     * of this method will have to call it again to attempt to make the check again.
     * <p>
     * The synchronizations in this method are made such that multiple calls to this
     * method will run concurrently, as long as different chunks are targeted for
     * splitting.
     * <p>
     * If two (or more) concurrent calls to this method target the same chunk, they
     * should be made with ranges that do not intersect. For instance, assuming this
     * collection holds a chunk mapped to range [0, 100). Calls to this method with
     * ranges [0,50) and [50, 75) and [90, 100) in whichever order (or concurrently)
     * is acceptable. However, calling this method with parameters [0, 50) and [25,
     * 75) is problematic as the second one to be made (or scheduled) will fail to
     * make the splits as the split points will be in two different chunks. However,
     * calling this method with parameters [0, 50) and later on with [25, 50) is
     * acceptable.
     *
     * @param lr the point at which there needs to be a change of chunk. This range
     *           needs to be empty, i.e. its members "from" and "to" need to be
     *           equal
     * @return {@code true} if the specified range can be safely sent to a remote
     *         place, {@code false} if this method needs to be called again to make
     *         it happen
     */
    private boolean attemptSplitChunkBeforeMoveAtSinglePoint(LongRange lr) {
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(lr);

        // It is possible for the requested point not to be present in any chunk
        if (entry == null || entry.getKey().contains(lr.from)) {
            return true;
        }

        final LongRange chunkRange = entry.getKey();
        final boolean splitNeeded = chunkRange.from < lr.from && lr.from < chunkRange.to;

        if (!splitNeeded) {
            return true;
        }

        // Arrived here, we know that the chunk we have needs to be split
        // We synchronize on this specific Chunk
        synchronized (entry) {
            // We restart the chunk acquisition process to check if we obtain the same chunk
            // If that is not the case, another thread has modified the chunks in the
            // ChunkedList and
            // this method has failed to do the modification, which will have to be
            // attempted again
            Map.Entry<LongRange, RangedList<T>> checkEntry = chunks.floorEntry(lr);
            if (checkEntry == null || checkEntry.getKey().to <= lr.from) {
                checkEntry = chunks.ceilingEntry(lr);
            }
            if (!entry.getKey().equals(checkEntry.getKey())) {
                return false;
            }

            // Check passed, we are the only thread which can split the targeted chunk
            final LinkedList<RangedList<T>> splittedChunks = entry.getValue().splitRange(lr.from);
            while (!splittedChunks.isEmpty()) {
                // It is important to insert the splitted chunks in reverse order.
                // Otherwise, parts of the original chunk would be shadowed due to the ordering
                // of Chunks used in ChunkedList, concurrently calling ChunkedList(or
                // DistCol)#get(long) would fail.
                add_unchecked(splittedChunks.pollLast());
            }
            remove(entry.getKey());
            return true;
        }
    }

    /**
     * Method used in preparation before transferring chunks. This method checks if
     * a chunk contained in this object has its range exactly matching the range
     * specified as parameter. If that is the case, returns {@code true}.
     * <p>
     * If that is not the case, i.e. a chunk held by this collection needs to be
     * split so that the specified range can be sent to a remote host, attempts to
     * make the split. If it is successful in splitting the existing chunk so that
     * the specified range has a corresponding chunk stored in this collection,
     * returns {@code true}. If splitting the existing range failed (due to a
     * concurrent attempts to split that range), returns {@code false}. The caller
     * of this method will have to call it again to attempt to make the check again.
     * <p>
     * The synchronizations in this method are made such that multiple calls to this
     * method will run concurrently, as long as different chunks are targeted for
     * splitting.
     * <p>
     * If two (or more) concurrent calls to this method target the same chunk, they
     * should be made with ranges that do not intersect. For instance, assuming this
     * collection holds a chunk mapped to range [0, 100). Calls to this method with
     * ranges [0,50) and [50, 75) and [90, 100) in whichever order (or concurrently)
     * is acceptable. However, calling this method with parameters [0, 50) and [25,
     * 75) is problematic as the second one to be made (or scheduled) will fail to
     * make the splits as the split points will be in two different chunks. However,
     * calling this method with parameters [0, 50) and later on with [25, 50) is
     * acceptable.
     *
     * @param lr the range of entries which is going to be sent away. It is assumed
     *           that there exists a chunk in this collection which includes this
     *           provided range.
     * @return {@code true} if the specified range can be safely sent to a remote
     *         place, {@code false} if this method needs to be called again to make
     *         it happen
     */
    private boolean attemptSplitChunkBeforeMoveAtTwoPoints(LongRange lr) {
        final Map.Entry<LongRange, RangedList<T>> entry = chunks.floorEntry(lr);

        final LongRange chunkRange = entry.getKey();
        final boolean leftSplit = chunkRange.from < lr.from;
        final boolean rightSplit = lr.to < chunkRange.to;

        long[] splitPoints;
        if (leftSplit && rightSplit) {
            splitPoints = new long[2];
            splitPoints[0] = lr.from;
            splitPoints[1] = lr.to;
        } else if (leftSplit) {
            splitPoints = new long[1];
            splitPoints[0] = lr.from;
        } else if (rightSplit) {
            splitPoints = new long[1];
            splitPoints[0] = lr.to;
        } else {
            return true;
        }

        // Arrived here, we know that the chunk we have needs to be split
        // We synchronize on this specific Chunk
        synchronized (entry) {
            // We restart the chunk acquisition process to check if we obtain the same chunk
            // If that is not the case, another thread has modified the chunks in the
            // ChunkedList and
            // this method has failed to do the modification, which will have to be
            // attempted again
            Map.Entry<LongRange, RangedList<T>> checkEntry = chunks.floorEntry(lr);
            if (checkEntry == null || checkEntry.getKey().to <= lr.from) {
                checkEntry = chunks.ceilingEntry(lr);
            }
            if (!entry.getKey().equals(checkEntry.getKey())) {
                return false;
            }

            // Check passed, we are the only thread which can split the targeted chunk
            final LinkedList<RangedList<T>> splittedChunks = entry.getValue().splitRange(splitPoints);
            while (!splittedChunks.isEmpty()) {
                // It is important to insert the splitted chunks in reverse order.
                // Otherwise, parts of the original chunk would be shadowed due to the ordering
                // of Chunks used in ChunkedList, concurrently calling ChunkedList(or
                // DistCol)#get(long) would fail.
                add_unchecked(splittedChunks.pollLast());
            }
            remove(entry.getKey());
            return true;
        }
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

        final ArrayList<RangedList<T>> chunksToMove = new ArrayList<>();
        // Two cases to handle here, whether the specified range fits into a single
        // existing chunk or whether it spans multiple chunks
        final Map.Entry<LongRange, RangedList<T>> lowSideEntry = chunks.floorEntry(range);
        if (lowSideEntry != null && lowSideEntry.getKey().from <= range.from && range.to <= lowSideEntry.getKey().to) {
            // The given range is included (or identical) to an existing Chunk.
            // Only one Chunk needs to be split (if any).
            while (!attemptSplitChunkBeforeMoveAtTwoPoints(range)) {
                ;
            }
            chunksToMove.add(chunks.get(range));
        } else {
            // The given range spans multiple ranges, the check on whether chunks need to be
            // split needs to be done separately on single points
            final LongRange leftSplit = new LongRange(range.from);
            final LongRange rightSplit = new LongRange(range.to);

            while (!attemptSplitChunkBeforeMoveAtSinglePoint(leftSplit)) {
                ;
            }
            while (!attemptSplitChunkBeforeMoveAtSinglePoint(rightSplit)) {
                ;
            }

            // Accumulate all the chunks that are spanned by the range specified as
            // parameter
            final NavigableSet<LongRange> keySet = chunks.keySet();
            LongRange rangeToAdd = keySet.ceiling(range);

            while (rangeToAdd != null && rangeToAdd.to <= range.to) {
                chunksToMove.add(chunks.get(rangeToAdd));
                rangeToAdd = keySet.higher(rangeToAdd);
            }
        }

        if (chunksToMove.isEmpty()) {
            return;
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

    // Method moved to GLOBAL and TEAM operations
    // @Override
    // public void distSize(long[] result) {
    // for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
    // final LongRange k = entry.getKey();
    // final Place p = entry.getValue();
    // result[manager.placeGroup.rank(p)] += k.size();
    // }
    // }

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
