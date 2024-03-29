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
import java.util.concurrent.ConcurrentHashMap;

import apgas.Place;
import handist.collections.ElementOverlapException;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializerUsingPlace;
import handist.collections.function.Serializer;

public class DistManager<T> {

    static class ParameterErrorException extends RuntimeException {
        /**
         *
         */
        private static final long serialVersionUID = -9038636040813564069L;
        public int reason;

        ParameterErrorException(int reason, String msg) {
            super(msg);
            this.reason = reason;
        }
    };

    /*
     * public static class Range extends DistManager<LongRange> { }
     */

    static class SystemError extends Error {
        /**
         *
         */
        private static final long serialVersionUID = 4466816572543219426L;
        public int reason;

        SystemError(int reason, String msg) {
            super(msg);
            this.reason = reason;
        }
    }

    public static final int DIST_ADDED = 1;
    /** Code for key received from remote place to this local handle */
    public static final int DIST_MOVED_IN = 4;
    public static final int DIST_REMOVED = 2;

    public static final byte MOVE_NEW = 1; // New range created on local handle
    public static final byte MOVE_NONE = 0;
    public static final byte MOVE_OLD = 2;

    /**
     * Changes since that last time updateDist was called that will need to be
     * notified to remote places. There may be other changes that occurred on remote
     * places that this local handle is not yet aware of.
     */
    public ConcurrentHashMap<T, Integer> diff = new ConcurrentHashMap<>();
    /** Current knowledge of the key-holding information on local & remote places */
    public ConcurrentHashMap<T, Place> dist = new ConcurrentHashMap<>();

    HashSet<T> importedDiffKeys = new HashSet<>();

    public void add(T key) throws ElementOverlapException {
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
                // !distIsLocal(key)
                reject("add", 105, key);
            }
        } else {
            if (diffHasKey(key)) {
                if (diffOfKeyIs(key, DIST_REMOVED)) {
                    diff.remove(key);
                    dist.put(key, here());
                } else {
                    systemError("add", 101, key);
                }
            } else {
                diff.put(key, DIST_ADDED);
                dist.put(key, here());
            }
        }
    }

    void applyDiff(T key, int operation, Place from) throws Exception {
        // System.out.println("[" + here.id + "] applyDiff " + key + " op: " + operation
        // + " from: " + from.id);
        if (importedDiffKeys.contains(key) || diff.containsKey(key)) {
            reject("applyDiff with duplicate key ", operation, key);
        } else {
            importedDiffKeys.add(key);
            if ((operation & (DIST_ADDED | DIST_MOVED_IN)) != 0) {
                dist.put(key, from);
            } else {
                // operation == DIST_REMOVED
                dist.remove(key);
            }
        }
    }

    public void clear() {
        dist.clear();
        diff.clear();
    }

    public boolean diffHasKey(T key) {
        return diff.containsKey(key);
    }

    public boolean diffOfKeyIs(T key, int operation) {
        return (diff.get(key) & operation) != 0;
    }

    public boolean distHasKey(T key) {
        return dist.containsKey(key);
    }

    public boolean distIsLocal(T key) {
        return dist.get(key) == here();
    }

    public void moveInNew(T key) throws Exception {
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
            }
        }
    }

    public void moveInOld(T key) throws Exception {
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
                }
            }
        } else {
            // !distHasKey(key)
            systemError("moveInOld", 405, key);
        }
    }

    public byte moveOut(T key, Place dest) {
        if (distHasKey(key)) {
            // System.out.println(">>> distHasKey");
            if (distIsLocal(key)) {
                if (diffHasKey(key)) {
                    if (diffOfKeyIs(key, DIST_ADDED)) {
                        diff.remove(key);
                        dist.remove(key);
                        return MOVE_NEW;
                    } else if (diffOfKeyIs(key, DIST_MOVED_IN)) {
                        diff.remove(key);
                        dist.put(key, dest);
                        return MOVE_OLD;
                    } else {
                        systemError("moveOut", 804, key);
                    }
                } else {
                    dist.put(key, dest);
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

    void reject(String method, int reason, T key) throws ParameterErrorException {
        final String msg = "[" + here() + "] Error when calling " + method + " " + key + " on code " + reason;
        System.err.println(msg);
        if (reason > 0) {
            throw new ParameterErrorException(reason, msg);
        }
        throw new ParameterErrorException(reason, msg);
    }

    public void remove(T key) {
        if (distHasKey(key)) {
            if (distIsLocal(key)) {
                if (diffHasKey(key)) {
                    // System.out.println("[" + here.id + "] remove key " + key);
                    if (diffOfKeyIs(key, DIST_ADDED)) {
                        diff.remove(key);
                        dist.remove(key);
                    } else if (diffOfKeyIs(key, DIST_MOVED_IN)) {
                        diff.put(key, DIST_REMOVED);
                        dist.remove(key);
                    } else {
                        systemError("remove", 202, key);
                    }
                } else {
                    diff.put(key, DIST_REMOVED);
                    dist.remove(key);
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
    void updateDist(TeamedPlaceGroup pg) {
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
        CollectiveRelocator.allgatherSer(pg, serProcess, desProcess);
        importedDiffKeys.clear();
        diff.clear();
    }

}
