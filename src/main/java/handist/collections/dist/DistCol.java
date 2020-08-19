/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import handist.collections.dist.util.Pair;
import handist.collections.function.DeSerializer;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.Serializer;

/**
 * A class for handling objects at multiple places. It is allowed to add new
 * elements dynamically. This class provides the method for load balancing.
 * <p>
 * Note: In the current implementation, there are some limitations.
 * <ul>
 *  <li>There is only one load balancing method: the method flattens the number of
 * elements of the all places.
 * </ul>
 *
 * @param <T> the type of elements handled by this {@link DistCol}
 */
@DefaultSerializer(JavaSerializer.class)
public class DistCol<T> extends ChunkedList<T> implements AbstractDistCollection<DistCol<T>>, RangeRelocatable<LongRange>, SerializableWithReplace {
	/*AbstractDistCollection *//* implements List[T], ManagedDistribution[LongRange] */
	static class ChunkExtractLeft<T> {
		public RangedList<T> original;
		public long splitPoint;

		ChunkExtractLeft(final RangedList<T> original, final long splitPoint) {
			this.original = original;
			this.splitPoint = splitPoint;
		}

		List<RangedList<T>> extract() {
			return original.splitRange(splitPoint);
		}
	}

	static class ChunkExtractMiddle<T> {
		public RangedList<T> original;
		public long splitPoint1;
		public long splitPoint2;

		ChunkExtractMiddle(final RangedList<T> original, final long splitPoint1, final long splitPoint2) {
			this.original = original;
			this.splitPoint1 = splitPoint1;
			this.splitPoint2 = splitPoint2;
		}

		List<RangedList<T>> extract() {
			return original.splitRange(splitPoint1, splitPoint2);
		}
	}
	static class ChunkExtractRight<T> {
		public RangedList<T> original;
		public long splitPoint;

		ChunkExtractRight(final RangedList<T> original, final long splitPoint) {
			this.original = original;
			this.splitPoint = splitPoint;
		}

		List<RangedList<T>> extract() {
			return original.splitRange(splitPoint);
		}
	}

	public class DistColGlobal extends GlobalOperations<DistCol<T>> {
	    public DistColGlobal(DistCol<T> handle) {
			super(handle);
		}

		@Override
		public void onLocalHandleDo(SerializableBiConsumer<Place, DistCol<T>> action) {
			global_setupBranches(action);
		}

		public void setupBranches(final SerializableBiConsumer<Place, DistCol<T>> gen) {
	    	global_setupBranches(gen); 
	    }
	    
	    
	}

	public class DistColTeam extends TeamOperations<DistCol<T>> {
		
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
	    public void updateDist() { team_updateDist(); }
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
                result[this.placeGroup.rank(p)] += k.size();
            }
        }

        @Override
        protected void moveAtSyncCount(ArrayList<IntLongPair> moveList, MoveManagerLocal mm) throws Exception {
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
	 * Handle to Global Operations implemented by {@link DistCol}.
	 */
    public final transient DistColGlobal GLOBAL;

    /**
     * Internal class that handles distribution-related operations.
     */
	protected final transient DistManager<LongRange> ldist;

	protected transient DistributionManager<T> manager;

	private Function<Long, T> proxyGenerator;
	
	/**
	 * Handle to Team Operations implemented by {@link DistCol}.
	 */
	public final transient DistColTeam TEAM;
	
	/**
	 * Create a new DistCol. All the hosts participating in the distributed
	 * computation are susceptible to handle the created instance. This
	 * constructor is equivalent to calling {@link #DistCol(TeamedPlaceGroup)}
	 * with {@link TeamedPlaceGroup#getWorld()} as argument.
	 */
	public DistCol() {
		this(TeamedPlaceGroup.getWorld());
	}
	
    public DistCol(final TeamedPlaceGroup placeGroup) {
		this(placeGroup,new GlobalID());
	}

	public DistCol(final TeamedPlaceGroup placeGroup, final GlobalID id) {
	    super();
	    manager=new DistributionManager<>(placeGroup, id, this);
	    manager.locality = initialLocality(placeGroup.size);
	    ldist = new DistManager<>(); 
		TEAM = new DistColTeam(this);
		GLOBAL = new DistColGlobal(this);
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

	/**
     * Return the value corresponding to the specified index.
     * 
     * If the specified index is not located on this place, a 
     * {@link IndexOutOfBoundsException} will be raised, except if a proxy 
     * generator was set for this instance, in which case the value generated by 
     * the proxy is returned.
     * 
     * @param index index whose value needs to be retrieved
     * @throws IndexOutOfBoundsException if the specified index is not contained
     * in this local collection and no proxy was defined
     * @return the value corresponding to the provided index, or the value 
     * generated by the proxy if it was defined and the specified index is 
     * outside the range of indices of this local instance
     * @see #setProxyGenerator(Function)
     */
	@Override
	public T get(long index) {
	    if(proxyGenerator==null) {
            return super.get(index);
	    } else {
	        try {
	            return super.get(index);
	        } catch (IndexOutOfBoundsException e) {
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

	public HashMap<LongRange, Place> getDist() {
		return ldist.dist;
	}

	public LongDistribution getDistributionLong() {
		return LongDistribution.convert(getDist());
	}

    public LongRangeDistribution getRangedDistributionLong() {
		return new LongRangeDistribution(getDist());
	}

	@Override
	public GlobalOperations<DistCol<T>> global() {
		return GLOBAL;
	}

	// TODO global ope. (not teamed).
	//TODO make private to force the use of global op
    private void global_setupBranches(final SerializableBiConsumer<Place, DistCol<T>> gen) {
		final DistCol<T> handle = this;
		finish(() -> {
			handle.manager.placeGroup.broadcastFlat(() -> {
				gen.accept(here(), handle);
			});
		});
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
	public void moveAtSync(final List<RangedList<T>> cs, final Place dest, final MoveManagerLocal mm) {
		if (_debug_level > 5) {
			System.out.print("[" + here().id + "] moveAtSync List[RangedList[T]]: ");
			for (final RangedList<T> rl : cs) {
				System.out.print("" + rl.getRange() + ", ");
			}
			System.out.println(" dest: " + dest.id);
		}

		if (dest.equals(here()))
			return;

		final DistCol<T> toBranch = this; // using plh@AbstractCol
		final Serializer serialize = (ObjectOutputStream s) -> {
			final ArrayList<Byte> keyTypeList = new ArrayList<>();
			for (final RangedList<T> c : cs) {
				keyTypeList.add(ldist.moveOut(c.getRange(), dest));
				this.removeForMove(c);
			}
			s.writeObject(keyTypeList);
			s.writeObject(cs);
		};
		final DeSerializer deserialize = (ObjectInputStream ds) -> {
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
	
	public void moveAtSyncCount(final ArrayList<IntLongPair> moveList, final MoveManagerLocal mm) throws Exception {
		// TODO ->LinkedList? sort??
		final ArrayList<LongRange> localKeys = new ArrayList<>();
		localKeys.addAll(ranges());
		localKeys.sort((LongRange range1, LongRange range2) -> {
			long len1 = range1.to - range1.from;
			long len2 = range2.to - range2.from;
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
			if (dest.equals(here()))
				continue;
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
	
	
	public void moveRangeAtSync(final Distribution<Long> dist, final MoveManagerLocal mm) {
		moveRangeAtSync((LongRange range) -> {
			ArrayList<Pair<Place, LongRange>> listPlaceRange = new ArrayList<>();
			for (final Long key : range) {
				listPlaceRange.add(new Pair<Place, LongRange>(dist.place(key), new LongRange(key, key + 1)));
			}
			return listPlaceRange;
		}, mm);
	}
	public void moveRangeAtSync(Function<LongRange, List<Pair<Place, LongRange>>> rule, MoveManagerLocal mm) {
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
    public void moveRangeAtSync(final LongRange range, final Place dest, final MoveManagerLocal mm) {
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
				if (cRange.to <= range.from) { //cRange.max < range.min) {
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
							chunksToExtractLeft.add(new ChunkExtractLeft<T>(c, range.to/*max + 1*/));
						}
					} else {
						// cRange.min < range.min
						if (range.to < cRange.to) {
							// split at range.min-1/range.min
							// split at range.max/range.max+1
							// add range.min..range.max
							chunksToExtractMiddle.add(new ChunkExtractMiddle<T>(c, range.from, range.to/*max + 1*/));
						} else {
							// split at range.min-1/range.min
							// cRange.max =< range.max
							// add range.min..cRange.max
							chunksToExtractRight.add(new ChunkExtractRight<T>(c, range.from));
						}
					}
				}
			} else {
				// range.min < cRange.min
				if (range.to <= cRange.from) { //range.max < cRange.min) {
					// skip
				} else {
					// cRange.min <= range.max
					if (cRange.to <= range.to) {
						// add cRange.min..cRange.max
						chunksToMove.add(c);
					} else {
						// split at range.max/range.max+1
						// add cRange.min..range.max
						chunksToExtractLeft.add(new ChunkExtractLeft<T>(c, range.to/*max + 1*/));
					}
				}
			}
		});

		for (final ChunkExtractLeft<T> chunkToExtractLeft : chunksToExtractLeft) {
			final RangedList<T> original = chunkToExtractLeft.original;
			final List<RangedList<T>> splits = chunkToExtractLeft.extract();
			//	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
			remove(original);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
			add(splits.get(0)/*first*/);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
			add(splits.get(1)/*second*/);
			chunksToMove.add(splits.get(0)/*first*/);
		}

		for (final ChunkExtractMiddle<T> chunkToExtractMiddle : chunksToExtractMiddle) {
			final RangedList<T> original = chunkToExtractMiddle.original;
			final List<RangedList<T>> splits = chunkToExtractMiddle.extract();
			//	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
			remove(original);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
			add(splits.get(0)/*first*/);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
			add(splits.get(1)/*second*/);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.third.getRange());
			add(splits.get(2)/*third*/);
			chunksToMove.add(splits.get(1)/*second*/);
		}

		for (final ChunkExtractRight<T> chunkToExtractRight : chunksToExtractRight) {
			final RangedList<T> original = chunkToExtractRight.original;
			final List<RangedList<T>> splits = chunkToExtractRight.extract();
			//	    System.out.println("[" + here.id + "] removeChunk " + original.getRange());
			remove(original);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.first.getRange());
			add(splits.get(0)/*first*/);
			//	    System.out.println("[" + here.id + "] putChunk " + splits.second.getRange());
			add(splits.get(1)/*second*/);
			chunksToMove.add(splits.get(1)/*second*/);
		}

		moveAtSync(chunksToMove, dest, mm);
	}

	public void moveRangeAtSync(final RangedDistribution<LongRange> dist, final MoveManagerLocal mm) throws Exception {
		moveRangeAtSync((LongRange range) -> {
			return dist.placeRanges(range);
		}, mm);
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

	@Override
	public RangedList<T> remove(final RangedList<T> c) {
		ldist.remove(c.getRange());
		return super.remove(c);
	}

//	Method moved to GLOBAL and TEAM operations 
//	@Override
//	public void distSize(long[] result) {
//		for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
//			final LongRange k = entry.getKey();
//			final Place p = entry.getValue();
//			result[manager.placeGroup.rank(p)] += k.size();
//		}
//	}

	private void removeForMove(final RangedList<T> c) {
		if (super.remove(c) == null) {
			throw new RuntimeException("DistCol#removeForMove");
		}
	}

	/**
     * Sets this instance's proxy generator.
     * 
     * The proxy feature is used to prepare an element when access to an index
     * that is not contained in the local range. Instead of throwing an 
     * {@link IndexOutOfBoundsException}, the value generated by the proxy will
     * be used. It resembles `getOrDefault(key, defaultValue)`.
     *
     * @param proxyGenerator function that takes a {@link Long} index as 
     * parameter and returns a T
     */
    public void setProxyGenerator(Function<Long,T> proxyGenerator) {
        this.proxyGenerator = proxyGenerator;
    }

	@Override
	public TeamOperations<DistCol<T>> team() {
		return TEAM;
	}

	// TODO make private
	private void team_updateDist() {
		ldist.updateDist(manager.placeGroup);
	}
	
	@Override
	public Object writeReplace() throws ObjectStreamException {
		final TeamedPlaceGroup pg1 = manager.placeGroup;
		final GlobalID id1 = manager.id;
		return new LazyObjectReference<DistCol<T>>(pg1, id1, () -> {
			return new DistCol<T>(pg1, id1);
		});
	}
}
