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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.function.Function;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.ElementOverlapException;
import handist.collections.LongRange;
import handist.collections.RangedList;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

public class DistCol<T> extends DistChunkedList<T> implements ElementLocationManagable<LongRange> {

    private static int _debug_level = 5;
    /**
     * Internal class that handles distribution-related operations.
     */
    protected final transient ElementLocationManager<LongRange> ldist;

    /**
     * Function kept and used when the local handle does not contain the specified
     * index in method {@link #get(long)}. This proxy will return a value to be
     * returned by the {@link #get(long)} method rather than throwing an
     * {@link Exception}.
     */
    private Function<Long, T> proxyGenerator;

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
    private DistCol(final TeamedPlaceGroup placeGroup, final GlobalID id) {
        this(placeGroup, id, (TeamedPlaceGroup pg, GlobalID gid) -> new DistCol<>(pg, gid));
    }

    protected DistCol(final TeamedPlaceGroup placeGroup, final GlobalID id,
            BiFunction<TeamedPlaceGroup, GlobalID, ? extends DistChunkedList<T>> lazyCreator) {
        super(placeGroup, id, lazyCreator);
        ldist = new ElementLocationManager<>();
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

    @Override
    public void clear() {
        // TODO
        // the current implementation assumes TEAMED operation of clear() and
        // does not support the situation where clear() is only called on some places.
        super.clear();
        ldist.clear();
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

    /*
     * Map<LongRange, Integer> getDiff() { return ldist.diff; }
     */

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
    public void getSizeDistribution(long[] result) {
        for (final Map.Entry<LongRange, Place> entry : ldist.dist.entrySet()) {
            final LongRange k = entry.getKey();
            final Place p = entry.getValue();
            result[manager.placeGroup.rank(p)] += k.size();
        }
    }

    @Override
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
        @SuppressWarnings("unchecked")
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

    private void putForMove(final RangedList<T> c, final byte mType) throws Exception {
        final LongRange key = c.getRange();
        switch (mType) {
        case ElementLocationManager.MOVE_NEW:
            ldist.moveInNew(key);
            break;
        case ElementLocationManager.MOVE_OLD:
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
        return this.remove(c.getRange());
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
    public void updateDist() {
        ldist.updateDist(manager.placeGroup);
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
