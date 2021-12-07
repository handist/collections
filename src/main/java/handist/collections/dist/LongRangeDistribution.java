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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import apgas.Place;
import handist.collections.ChunkedList;
import handist.collections.ChunkedList.LongRangeOrdering;
import handist.collections.LongRange;

/**
 * Class tracking the distribution of a collection whose entries are identified
 * by {@link LongRange}. Currently, this is limited to class {@link DistCol} but
 * may be extended to other distributed collections in the future.
 * <p>
 * This class allows users to inquire about the location of particular entries
 * of a distributed collection through methods {@link #rangeLocation(LongRange)},
 * {@link #location(Long)}, and {@link #location(LongRange)}. Internally, this
 * information is kept in a {@link Map}&lt;{@link LongRange},{@link Place}&gt;.
 * <p>
 * This class is an {@link UpdatableDistribution}, meaning it can be registered
 * into a collection so that it is automatically updated when changes in the
 * distribution occur, i.e. using
 * {@link DistCol#registerDistribution(UpdatableDistribution)}. It may even be
 * safely registered into multiple distributed collections provided that the
 * collections do not share any {@link LongRange} "keys".
 * <p>
 * To maintain the integrity of the distribution with respect to the underlying
 * distributed collection(s), this object cannot be modified by the user.
 *
 * @author Patrick Finnerty
 *
 */
public class LongRangeDistribution extends UpdatableDistribution<LongRange>
        implements RangedDistribution<LongRange>, Distribution<Long> {

    /** Serial Version UID */
    private static final long serialVersionUID = 2646369287127470136L;
    /**
     * Internal Sorted Map used to keep track of the location of each chunk of the
     * underlying {@link DistCol}.
     */
    private final TreeMap<LongRange, Place> distribution;

    /**
     * Default constructor. Creates a blank distribution which may be registered
     * into a distributed collection to track its distribution.
     */
    public LongRangeDistribution() {
        distribution = new TreeMap<>(new ChunkedList.LongRangeOrdering());
    }

    /**
     *
     * @param map map from {@link LongRange} to {@link Place} describing a
     *            distribution
     */
    public LongRangeDistribution(Map<LongRange, Place> map) {
        distribution = new TreeMap<>(new LongRangeOrdering());
        distribution.putAll(map);
    }

    /**
     * Constructor for internal use only.
     *
     * Creates a new {@link LongRangeDistribution} with the provided
     * {@link TreeMap}. It is assumed that the provided map uses the
     * {@link ChunkedList.LongRangeOrdering} to sort its keys. If it is not the
     * case, this constructor will throw an {@link IllegalArgumentException}.
     *
     *
     * @param map the map to use to build this {@link LongRangeDistribution}.
     * @throws IllegalArgumentException if the provided map does not use the
     *                                  appropriate ordering
     * @see #clone()
     */
    protected LongRangeDistribution(TreeMap<LongRange, Place> map) {
        if (!(map.comparator() instanceof LongRangeOrdering)) {
            throw new IllegalArgumentException(
                    "The provided map should use ChunkedList.LongRangeOrdering to sort its keys");
        }
        distribution = map;
    }

    /**
     * Returns a copy of this distribution by allocating a new
     * {@link LongRangeDistribution} object. Any subsequent changes made to this
     * object will NOT be reflected into the returned copy and vice-versa.
     */
    @SuppressWarnings("unchecked")
    @Override
    public LongRangeDistribution clone() {
        return new LongRangeDistribution((TreeMap<LongRange, Place>) distribution.clone());
    }

    /**
     * Creates a copy of the current distribution and returns it as a map from
     * {@link LongRange} to {@link Place}. The returned map may be modified freely
     * without any adverse influence on the integrity of this instance.
     *
     * @return a copy of the map used internally to keep track of the distribution
     */
    @SuppressWarnings("unchecked")
    public Map<LongRange, Place> getDistribution() {
        return (Map<LongRange, Place>) distribution.clone();
    }

    @Override
    public Place location(Long key) {
        final LongRange lr = new LongRange(key);
        final Map.Entry<LongRange, Place> entry = distribution.floorEntry(lr);
        if (entry == null || !entry.getKey().contains(key)) {
            throw new IndexOutOfBoundsException(
                    "LongRangeDistribution (as LongDistribution): " + key + " is not within the range of any chunk");
        }
        return entry.getValue();
    }

    /**
     * Provides the location of the specified range. It is assumed that the
     * specified range is exactly contained in this distribution. If the targeted
     * range does not exist in the collection, or if it has been split into 2 or
     * more chunks, this method will return null.
     * <p>
     * In cases where a range may have been split into several pieces, consider
     * using method {@link #rangeLocation(LongRange)} to obtain the location of each
     * piece.
     *
     * @param lr the range whose location needs to be returned
     * @return the location of the specified range, or null is this exact range is
     *         not contained in the distributed collection
     */
    public Place location(LongRange lr) {
        return distribution.get(lr);
    }

    @Override
    public Map<LongRange, Place> rangeLocation(LongRange range) {
        final Map<LongRange, Place> listPlaceRange = new HashMap<>();
        for (final LongRange mappedRange : distribution.keySet()) {
            final Place mappedPlace = distribution.get(mappedRange);
            if (mappedRange.from <= range.from) {
                if (range.from < mappedRange.to) { // if (range.min <= mappedRange.max) {
                    if (range.to <= mappedRange.to) {
                        listPlaceRange.put(range, mappedPlace);
                    } else {
                        listPlaceRange.put(new LongRange(range.from, mappedRange.to), mappedPlace);
                    }
                }
            } else {
                if (mappedRange.from < range.to) { // if (mappedRange.min <= range.max) {
                    if (range.to <= mappedRange.to) {
                        listPlaceRange.put(new LongRange(mappedRange.from, range.to), mappedPlace);
                    } else {
                        listPlaceRange.put(mappedRange, mappedPlace);
                    }
                }
            }
        }
        return listPlaceRange;
    }

    @Override
    void removeLocation(LongRange k) {
        distribution.remove(k);
    }

    @Override
    void updateLocation(LongRange k, Place location) {
        distribution.put(k, location);
    }
}
