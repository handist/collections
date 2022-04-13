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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

import apgas.Place;
import handist.collections.ElementOverlapException;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.Serializer;

/**
 * This class manages the elements of a distributed collection in a
 * difference-basis. It is meant to be used as a member on each handle of a
 * distributed collection whose distribution needs to be tracked.
 *
 * @param <T> the type of the object used as index or key to identify entries in
 *            the collection whose distribution is being tracked.
 */
class ElementLocationManager<T> {

    /**
     * Error class used when inconsistencies in the distribution attributable to a
     * forbidden operation made by the user are encountered.
     */
    static class ParameterErrorException extends RuntimeException {
        /** Serial Version UID */
        private static final long serialVersionUID = -9038636040813564069L;
        /** Integer code describing the cause of the problem */
        public int reason;

        ParameterErrorException(int reason, String msg) {
            super(msg);
            this.reason = reason;
        }
    };

    /**
     * Error class used when an internal error occurred in the
     * ElementLocationManager. Assuming that direct modifications to members
     * {@link #diff} and {@link #dist} are not made and that this class'
     * implementation is correct, this error should never be thrown.
     */
    static class SystemError extends Error {
        /** Serial Version UID */
        private static final long serialVersionUID = 4466816572543219426L;
        /** Integer Code describing the nature of the problem */
        public int reason;

        SystemError(int reason, String msg) {
            super(msg);
            this.reason = reason;
        }
    }

    /** Code used when a key is added to the local handle of the collection */
    static final int DIST_ADDED = 1;
    /** Code for key received from remote place to this local handle */
    static final int DIST_MOVED_IN = 4;
    /** Code used when a key is removed from the local handle of the collection */
    static final int DIST_REMOVED = 2;

    /**
     * Code used to describe that a newly added entry to the local collection (code
     * {@link #DIST_ADDED} in {@link #diff}) was relocated to a remote host
     */
    static final byte MOVE_NEW = 1;
    /**
     * Code used to describe that an entry that was previously relocated to some
     * place(s) has now returned to its original location.
     */
    static final byte MOVE_NONE = 0;
    /**
     * Code used to describe that an entry already known to all local handles was
     * relocated
     */
    static final byte MOVE_OLD = 2;

    /**
     * Changes since that last time updateDist was called that will need to be
     * notified to remote places. There may be other changes that occurred on remote
     * places that this local handle is not yet aware of.
     */
    ConcurrentHashMap<T, Integer> diff = new ConcurrentHashMap<>();
    /** Current knowledge of the key-holding information on local & remote places */
    ConcurrentHashMap<T, Place> dist = new ConcurrentHashMap<>();

    HashSet<T> importedDiffKeys = new HashSet<>();

    /**
     * WeakHashMap used to contain the distribution which need to receive updates
     * when changes to the distribution are made.
     * <h2>Implementation notes</h2>
     * <p>
     * A {@link WeakHashMap} is used with "null" values is used so that the
     * distribution objects used as keys in this collection can be garbage-collected
     * if they are no longer in use in other parts of the program, as represented in
     * {@link ElementLocationManageable#registerDistribution(UpdatableDistribution)}.
     * Refer to the Java documentation of {@link WeakHashMap} for further details.
     */
    protected Map<UpdatableDistribution<T>, Object> registeredDistribution = new WeakHashMap<>();

    /**
     * Registers the fact that a new key / entry was added to the local collection.
     *
     * @param key the object which identifies the newly added entry to the
     *            collection
     * @throws ElementOverlapException if the added key is known to be present on a
     *                                 remote host
     */
    void add(T key) throws ElementOverlapException {
        if (distHasKey(key)) {
            if (distIsLocal(key)) {
                if (diffHasKey(key)) {
                    if (diffOfKeyIs(key, (DIST_ADDED | DIST_MOVED_IN))) {
                        reject("add", 103, key);
                    } else {
                        systemError("add", 104, key);
                    }
                } else {
                    reject("add", 102, key);
                }
            } else {
                reject("add", 105, key);
            }
        } else {
            if (diffHasKey(key)) {
                if (diffOfKeyIs(key, DIST_REMOVED)) {
                    diff.remove(key);
                    dist.put(key, here());
                    registeredDistributionUpdate(key, here());
                } else {
                    systemError("add", 101, key);
                }
            } else {
                diff.put(key, DIST_ADDED);
                dist.put(key, here());
                registeredDistributionUpdate(key, here());
            }
        }
    }

    /**
     * This method is part of the procedure needed to update the local knwoledge of
     * the global distribution. A call to this method updates the contents of the
     * local distribution knowledge with the knowledge received from remote places.
     * Checks are made to make sure that the received individual change is
     * consistent with information obtained from other hosts using member
     * {@link #importedDiffKeys}.
     *
     *
     * @param key       the key about which some information was received
     * @param operation the operation that the remote operation registered about
     *                  this key
     * @param from      the place from which the present information came from
     * @throws Exception if inconsistencies are detected, such as a remote place
     *                   adding an entry already owned by another place ("duplicate
     *                   key" case)
     * @see #update(TeamedPlaceGroup)
     */
    void applyDiff(T key, int operation, Place from) throws Exception {
        // System.out.println("[" + here.id + "] applyDiff " + key + " op: " + operation
        // + " from: " + from.id);
        if (importedDiffKeys.contains(key) || diff.containsKey(key)) {
            reject("applyDiff with duplicate key ", operation, key);
        } else {
            importedDiffKeys.add(key);
            if ((operation & (DIST_ADDED | DIST_MOVED_IN)) != 0) {
                dist.put(key, from);
                registeredDistributionUpdate(key, from);
            } else {
                // operation == DIST_REMOVED
                dist.remove(key);
                registeredDistributionRemove(key);
            }
        }
    }

    /**
     * Removes all distribution knowledge of this local instance. This method should
     * only be called when all entries in the local handle of the distributed
     * collection are removed.
     */
    void clear() {
        dist.clear();
        diff.clear();
        // FIXME this is not correct.
        // In diff, the fact that all local entries have been removed needs to be
        // recorded.
        // Also, the registered distributions need to be updated accordingly.
    }

    protected boolean diffHasKey(T key) {
        return diff.containsKey(key);
    }

    protected boolean diffOfKeyIs(T key, int operation) {
        return (diff.get(key) & operation) != 0;
    }

    protected boolean distHasKey(T key) {
        return dist.containsKey(key);
    }

    protected boolean distIsLocal(T key) {
        return dist.get(key) == here();
    }

    protected void moveInNew(T key) throws Exception {
        // System.out.println(">>> moveInNew " + key + " distHasKey: " + distHasKey(key)
        // + " diffHasKey: " + diffHasKey(key));

        if (distHasKey(key)) {
            if (distIsLocal(key)) {
                if (diffHasKey(key) && diffOfKeyIs(key, DIST_ADDED)) {
                    reject("moveInNew", 402, key);
                } else {
                    systemError("moveInNew", 403, key);
                }
            } else {
                // !distIsLocal(key)
                if (diffHasKey(key)) {
                    systemError("moveInNew", 404, key);
                } else {
                    // System.out.println(">>> AAA");
                    diff.put(key, DIST_ADDED);
                    dist.put(key, here());
                    registeredDistributionUpdate(key, here());
                }
            }
        } else {
            // !distHasKey(key)
            if (diffHasKey(key)) {
                systemError("moveInNew", 401, key);
            } else {
                // System.out.println(">>> BBB");
                diff.put(key, DIST_ADDED);
                dist.put(key, here());
                registeredDistributionUpdate(key, here());
            }
        }
    }

    void moveInOld(T key) throws Exception {
        if (distHasKey(key)) {
            if (distIsLocal(key)) {
                systemError("moveInOld", 406, key);
            } else {
                // !distIsLocal(key)
                if (diffHasKey(key)) {
                    systemError("moveInOld", 407, key);
                } else {
                    diff.put(key, DIST_MOVED_IN);
                    dist.put(key, here());
                    registeredDistributionUpdate(key, here());
                }
            }
        } else {
            // !distHasKey(key)
            systemError("moveInOld", 405, key);
        }
    }

    byte moveOut(T key, Place dest) {
        if (distHasKey(key)) {
            // System.out.println(">>> distHasKey");
            if (distIsLocal(key)) {
                if (diffHasKey(key)) {
                    if (diffOfKeyIs(key, DIST_ADDED)) {
                        diff.remove(key);
                        dist.remove(key);
                        registeredDistributionRemove(key);
                        return MOVE_NEW;
                    } else if (diffOfKeyIs(key, DIST_MOVED_IN)) {
                        diff.remove(key);
                        dist.put(key, dest);
                        registeredDistributionUpdate(key, dest);
                        return MOVE_OLD;
                    } else {
                        systemError("moveOut", 804, key);
                    }
                } else {
                    dist.put(key, dest);
                    registeredDistributionUpdate(key, dest);
                    return MOVE_OLD;
                }
            } else {
                // !distIsLocal(key)
                reject("moveOut", 805, key);
            }
        } else {
            // !distHasKey(key)
            // System.out.println(">>> !distHasKey");
            if (diffHasKey(key)) {
                // System.out.println(">>> diffHasKey");
                if (diffOfKeyIs(key, DIST_REMOVED)) {
                    reject("moveOut", 802, key);
                } else {
                    systemError("moveOut", 803, key);
                }
            } else {
                // System.out.println(">>> !diffHasKey");
                reject("moveOut", 801, key);
            }
        }
        // System.out.println(">>> MOVE_NONE");
        return MOVE_NONE;
    }

    /**
     * Registers a distribution that will be actively updated by the
     * {@link ElementLocationManageable} from now on. The registered distribution
     * will receive updates until it is garbage-collected as it is internally kept
     * in a "weak" collection.
     * <p>
     * The contents of the given distribution are updated with the current knowledge
     * of this class when this method is called.
     * <p>
     * It is best to avoid inserting and removing entries of a distributed
     * collection when calling this method. This may cause some inconsistencies
     * between the information held by this instance and the information contained
     * in the distribution given as parameter.
     *
     * @param distributionToUpdate distribution into which changes in distribution
     *                             managed by this object need to be reflected
     */
    void registerDistribution(UpdatableDistribution<T> distributionToUpdate) {
        // Update the contents with the current knowledge of the situation
        for (final Map.Entry<T, Place> distEntry : dist.entrySet()) {
            distributionToUpdate.updateLocation(distEntry.getKey(), distEntry.getValue());
        }

        // Register the distribution to reflect the changes from now on
        registeredDistribution.put(distributionToUpdate, null);
    }

    /**
     * Sub-routine used to update the registered distributions with the fact that a
     * key has been removed from the collection
     *
     * @param key the key removed from the collection
     */
    protected void registeredDistributionRemove(T key) {
        for (final UpdatableDistribution<T> distribution : registeredDistribution.keySet()) {
            distribution.removeLocation(key);
        }
    }

    /**
     * Sub-routine used to update the location of an entry into all the registered
     * distribution contained
     *
     * @param key      the key to update
     * @param location the location of the specified key
     */
    protected void registeredDistributionUpdate(T key, Place location) {
        for (final UpdatableDistribution<T> distribution : registeredDistribution.keySet()) {
            distribution.updateLocation(key, location);
        }
    }

    void reject(String method, int reason, T key) throws ParameterErrorException {
        final String msg = "[" + here() + "] Error when calling " + method + " " + key + " on code " + reason;
        System.err.println(msg);
        throw new ParameterErrorException(reason, msg);
    }

    void remove(T key) {
        if (distHasKey(key)) {
            if (distIsLocal(key)) {
                if (diffHasKey(key)) {
                    // System.out.println("[" + here.id + "] remove key " + key);
                    if (diffOfKeyIs(key, DIST_ADDED)) {
                        diff.remove(key);
                        dist.remove(key);
                        registeredDistributionRemove(key);
                    } else if (diffOfKeyIs(key, DIST_MOVED_IN)) {
                        diff.put(key, DIST_REMOVED);
                        dist.remove(key);
                        registeredDistributionRemove(key);
                    } else {
                        systemError("remove", 202, key);
                    }
                } else {
                    diff.put(key, DIST_REMOVED);
                    dist.remove(key);
                    registeredDistributionRemove(key);
                }
            } else {
                // !distIsLocal(key)
                reject("remove", 203, key);
            }
        } else {
            // !distHasKey(key)
            reject("remove", 201, key);
        }
    }

    void setup(Collection<T> keys) {
        assert (keys.isEmpty());
        try {
            for (final T k : keys) {
                add(k);
            }
        } catch (final Exception e) {
            throw new RuntimeException("[DistManager] Duplicate key in " + keys);
        }
    }

    void systemError(String method, int reason, T key) throws SystemError {
        final String msg = "[" + here() + "] System Error when calling " + method + " " + key + " on code " + reason;
        System.err.println(msg);
        if (reason > 0) {
            throw new SystemError(reason, msg);
        }
        throw new SystemError(reason, msg);
    }

    @Override
    public String toString() {
        return "[DistManager] + dist: " + dist + ",  diff: " + diff + ", imported: " + importedDiffKeys + "-----";
    }

    @SuppressWarnings("unchecked")
    void update(TeamedPlaceGroup pg) {
        final Serializer serProcess = (ObjectOutput s) -> {
            s.writeObject(diff);
        };
        final DeSerializerUsingPlace desProcess = (ObjectInput ds, Place from) -> {
            final Map<T, Integer> importedDiff = (Map<T, Integer>) ds.readObject();
            for (final Map.Entry<T, Integer> entry : importedDiff.entrySet()) {
                final T k = entry.getKey();
                final Integer v = entry.getValue();
                applyDiff(k, v, from);
            }
        };
        new CollectiveRelocator.Allgather(pg).request(serProcess, desProcess).execute();
        importedDiffKeys.clear();
        diff.clear();
    }

}
