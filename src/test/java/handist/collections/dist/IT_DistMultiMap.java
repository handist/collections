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
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import apgas.MultipleException;
import apgas.Place;
import handist.collections.function.SerializableFunction;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_DistMultiMap implements Serializable {

    /** Total number of Strings stored in the multimap */
    static final int numData = 1000;

    /** Total number of keys in the multimap */
    static final int numKey = 100;

    /** Serial Version UID */
    private static final long serialVersionUID = -699276324622147605L;

    /** Number of entries for each key */
    static final int totalEntriesPerKey = numData / numKey;

    /** Instance under test */
    DistMultiMap<String, String> distMultiMap;
    ArrayList<String> keyList;
    /** Number of hosts participating in the test */
    int NPLACES;

    /** Place Group on which the test takes place */
    TeamedPlaceGroup pg = TeamedPlaceGroup.getWorld();

    Random random;

    public String genRandStr(String header) {
        final long rand = random.nextLong();
        return header + rand;
    }

    @Test
    public void run() throws Throwable {
        // Create initial data at Place 0
        try {
            for (long i = 0; i < numKey; i++) {
                keyList.add(genRandStr("r"));
            }
            long j = 0;
            for (long i = 0; i < numData; i++) {
                distMultiMap.put1(keyList.get((int) j), genRandStr("v"));
                j = (j + 1) % numKey;
            }
        } catch (final Exception e) {
            System.err.println("Error on " + here());
            e.printStackTrace();
        }
        // Check each place has the expected number of entries
        x_checkSize((j) -> {
            return j == 0 ? numKey : 0;
        }, (s) -> {
            return totalEntriesPerKey;
        });

        // Distribute all entries
        z_distribute();

        // From now on we track the number of keys on each rank of the computation
        // This array will be used repeatedly to check
        final int keyCount[] = new int[pg.size()];
        final long[] temporaryArray = new long[pg.size];
        distMultiMap.GLOBAL.getSizeDistribution(temporaryArray);
        for (int i = 0; i < pg.size; i++) {
            keyCount[i] = (int) temporaryArray[i];
            // System.out.println("On rank " + i + " " + keyCount[i]);
        }

        // Check again that each place has the expected number of entries
        x_checkSize((i) -> {
            return keyCount[i];
        }, (s) -> {
            return totalEntriesPerKey;
        }); // Is a bit redundant
        x_checkKeyShift(0);

        // ------------------------------------------------------------------------------
        // Move all entries to the next place
        z_moveToNextPlace();

        // Check that entries have indeed shifted by one place
        x_checkSize((i) -> {
            return keyCount[(i - 1 + NPLACES) % NPLACES];
        }, (s) -> {
            return totalEntriesPerKey;
        }); // Is a bit redundant
        x_checkKeyShift(NPLACES - 1);

        // ---------------------------------------------------------------------------

        // Add extra values on place 0
        // This adds numData values on place 0 only
        // As the new entries are placed on all possible keys, Place 0 has all #numKey
        // keys manipulated by keys on its local handle.
        try {
            long j = numData % numKey; // getter for key
            for (long i = 0; i < numData; i++) {
                distMultiMap.put1(keyList.get((int) j), genRandStr("x"));
                j = (j + 1) % numKey;
            }
        } catch (final Exception e) {
            System.err.println("Error on " + here());
            e.printStackTrace();
        }

        x_checkSize((r) -> {
            return r == 0 ? numKey : keyCount[(r - 1 + NPLACES) % NPLACES];
        }, (s) -> {
            if (pg.rank() == 0) {
                // Could be entriesPerKey or double depending on the key
                return z_shift(s) == NPLACES - 1 ? 2 * totalEntriesPerKey : totalEntriesPerKey;
            } else {
                return totalEntriesPerKey;
            }
        });

        // Move entries on even ranks to the next odd rank
        pg.broadcastFlat(() -> {
            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                if (pg.rank() % 2 == 0) {
                    final int rank = pg.rank(here());
                    // Assumption that there is an even number of hosts running this test
                    final Place destination = pg.get(rank + 1);
                    distMultiMap.forEach1((String key, String value) -> {
                        distMultiMap.moveAtSync(key, destination, mm);
                    });
                }
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });

        // No entries on even ranks, more entries on odd ranks
        x_checkSize((r) -> {
            if (r % 2 == 0) {
                return 0;
            }
            if (r == 1) {
                // Rank 1 has all possible keys because of generation of extra entries on rank 0
                return 100;
            } else {
                // Odd ranks cummulate the keys of both
                return keyCount[(r - 1 + NPLACES) % NPLACES] + keyCount[(r - 2 + NPLACES) % NPLACES];
            }
        }, (s) -> {
            // On rank one, the situation is confusing as there are values that:
            // - were originally on 0 and were transferred with z_moveToNextPlace
            // - were transferred from 3 to 0 with z_moveToNextPlace and are now on 1 thanks
            // to the transfer from even ranks
            // - all the entries that were added on host 0 that are now on 1 thanks to the
            // transfer from even ranks
            if (pg.rank() == 1 && (z_shift(s) == 0) || (z_shift(s) == NPLACES - 1)) {
                return 2 * totalEntriesPerKey;
            } else {
                return totalEntriesPerKey;
            }
        });

        // ---------------------------------------------------------------------------
        // Move all entries back to place 0
        pg.broadcastFlat(() -> {
            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                final Place destination = pg.get(0);
                distMultiMap.forEach1((String key, String value) -> {
                    distMultiMap.moveAtSync(key, destination, mm);
                });
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });

        // In the end there are twice the number of entries for each key on rank 0
        x_checkSize((r) -> {
            return r == 0 ? numKey : 0;
        }, (s) -> {
            return 2 * totalEntriesPerKey;
        });
    }

    @Before
    public void setup() {
        NPLACES = pg.size();
        random = new Random(12345);
        distMultiMap = new DistMultiMap<>(pg);
        keyList = new ArrayList<>();
    }

    @After
    public void tearDown() {
        distMultiMap.destroy();
    }

    /**
     * Checks that the hashCode of each key in the multimap matches the rank on
     * which they are located (with the specified shift).
     *
     * @param expectedShift integer corresponding to the number of times entries
     *                      were transfered from a place to the next
     * @throws Throwable if thrown during the call
     */
    private void x_checkKeyShift(int expectedShift) throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final int PLACES = distMultiMap.placeGroup.size();
                final int shift = (expectedShift + pg.rank()) % PLACES;
                try {
                    // Check that each key/pair is on the right place
                    for (final String key : distMultiMap.getAllKeys()) {
                        final int hash = Math.abs(key.hashCode());
                        final int apparentShift = (hash % PLACES);
                        assertEquals(shift, apparentShift);
                    }
                } catch (final Throwable e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Checks that each rank participating in this test holds the expected number of
     * keys, and number of entries per key
     *
     * @param s
     */
    private void x_checkSize(SerializableFunction<Integer, Integer> expectedKey,
            SerializableFunction<String, Integer> entryPerKey) throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final int keysExpected = expectedKey.apply(pg.rank());
                assertEquals(
                        "Expected " + keysExpected + " keys on rank " + pg.rank() + "  but was " + distMultiMap.size(),
                        keysExpected, distMultiMap.size());

                for (final Entry<String, List<String>> e : distMultiMap.entrySet()) {
                    final int expectedListSize = entryPerKey.apply(e.getKey());
                    final int actualListSize = e.getValue().size();
                    assertEquals(
                            "Expected " + expectedListSize + " elements for key " + e.getKey() + "(shift "
                                    + z_shift(e.getKey()) + ") on rank " + pg.rank() + " but was " + actualListSize,
                            expectedListSize, actualListSize);
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Distribute all entries of the map
     *
     * @throws Throwable if thrown during runtime
     */
    private void z_distribute() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                try {
                    final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                    distMultiMap.forEach1((String key, String value) -> {
                        final int d = z_shift(key);
                        distMultiMap.moveAtSync(key, pg.get(d), mm);
                    });
                    mm.sync();
                } catch (final Exception e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    private void z_moveToNextPlace() {
        pg.broadcastFlat(() -> {
            try {
                final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                final int rank = pg.rank(here());
                final Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);

                distMultiMap.forEach1((String key, String value) -> {
                    distMultiMap.moveAtSync(key, destination, mm);
                });
                mm.sync();
            } catch (final Exception e) {
                System.err.println("Error on " + here());
                e.printStackTrace();
                throw e;
            }
        });
    }

    private int z_shift(String key) {
        final int h = key.hashCode();
        final int d = Math.abs(h) % NPLACES;
        return d;
    }
}
