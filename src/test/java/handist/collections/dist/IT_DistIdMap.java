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
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_DistIdMap implements Serializable {

    static final int NPLACES = places().size();

    /** Number of entries to place in the map */
    static final long numData = 200;

    /** Random instance used to populate the map */
    static Random random = new Random(12345);

    /** Serial Version UID */
    private static final long serialVersionUID = -6870647874233619233L;

    /**
     * Helper method to generate strings with the specified prefix
     *
     * @param prefix prefix of the random string returned
     * @return a random string with the specified prefix
     */
    public static String genRandStr(String prefix) {
        final long rand = random.nextLong();
        return prefix + rand;
    }

    /** distIdMap which is the object of the tests of this class */
    DistIdMap<String> distIdMap;

    /** PlaceGroup on which the distributed id map is defined */
    TeamedPlaceGroup pg;

    /**
     * Single test method of this class.
     *
     * @throws Throwable if thrown during the test
     */
    @Test(timeout = 20000)
    public void run() throws Throwable {
        // Create initial data at Place 0
        try {
            for (long i = 0; i < numData; i++) {
                distIdMap.put(i, genRandStr("v"));
            }
        } catch (final Exception e) {
            System.err.println("Error on " + here());
            e.printStackTrace();
            throw e.getSuppressed()[0];
        }

        // Check that the entries were correctly added to the collection on place 0
        x_checkSize((h) -> {
            return h.id == 0 ? numData : 0;
        });

        // ---------------------------------------------------------------------------
        // Distribute all the keys over the places
        z_distribute();
        // Check that every place got the same number of keys
        x_checkSize((h) -> {
            return numData / NPLACES;
        });
        // Check that the correct keys were transferred
        x_checkKeyShift(0l);

        for (long shift = 1; shift <= 2; shift++) {
            // Move all entries to the n+1%NB_PLACE place
            z_moveToNextPlace();
            // Number of keys on each host is unchanged

            x_checkSize((h) -> {
                return numData / NPLACES;
            });
            // Keys have now shifted by "shift"
            x_checkKeyShift(shift);
        }

        // ---------------------------------------------------------------------------
        // Move all entries to place 0
        z_moveToPlaceZero();
        // Check that all the entries are now on 0
        x_checkSize((h) -> {
            return h.id == 0 ? numData : 0;
        });

        // ---------------------------------------------------------------------------

        // Generate additional key/value pair
        try {
            for (long i = numData; i < numData * 2; i++) {
                distIdMap.put(i, genRandStr("v"));
            }
        } catch (final Exception e) {
            System.err.println("Error on " + here());
            e.printStackTrace();
            throw e;
        }

        // Distribute all entries with the additional key/values
        z_distribute();
        // As the number of entries have doubled,
        x_checkSize((h) -> {
            return numData * 2 / NPLACES;
        });
        x_checkKeyShift(0l);

        // Then remove additional key/value
        try {
            pg.broadcastFlat(() -> {
                final ArrayList<Long> keyList = new ArrayList<>();
                try {
                    distIdMap.forEach((Long key, String value) -> {
                        if (key >= numData) {
                            keyList.add(key);
                        }
                    });
                    for (final long key : keyList) {
                        distIdMap.remove(key);
                    }
                } catch (final Exception e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }

        // Removing the additional entries should have returned each local handle to
        // original nb entries.
        x_checkSize((h) -> {
            return numData / NPLACES;
        });
        // Key / place shift should be unchanged
        x_checkKeyShift(0l);
    }

    @Before
    public void setup() {
        pg = TeamedPlaceGroup.getWorld();
        distIdMap = new DistIdMap<>(pg);
    }

    @After
    public void tearDown() {
        distIdMap.destroy();
    }

    private void x_checkKeyShift(long expectedShift) throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final long shift = (expectedShift + here().id) % NPLACES;
                try {
                    // Check that each key/pair is on the right place
                    for (final Long key : distIdMap.getAllKeys()) {
                        // long chunkNumber = lr.from / (rangeSize + rangeSkip);
                        final long apparentShift = (key % NPLACES);
                        assertEquals(shift, apparentShift);
                    }
                } catch (final Throwable e) {
                    final RuntimeException re = new RuntimeException("Error on " + here());
                    re.initCause(e);
                    throw re;
                }
            });
        } catch (final MultipleException me) {
            me.printStackTrace();
            throw me.getSuppressed()[0];
        }
    }

    /**
     * Subroutine which checks that every place holds half of the total instances
     *
     * @param size indicates the expected size as a function of the place provided
     *             as parameter
     * @throws Throwable if thrown during the check
     */
    private void x_checkSize(final SerializableFunction<Place, Long> size) throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                final long expected = size.apply(here());
                try {
                    assertEquals(expected, distIdMap.size());
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

    private void z_distribute() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                try {
                    final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                    distIdMap.forEach((Long key, String value) -> {
                        final int d = (int) (key % pg.size());
                        distIdMap.moveAtSync(key, pg.get(d), mm);
                    });
                    mm.sync();
                } catch (final Exception e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    private void z_moveToNextPlace() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                try {
                    final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                    final int rank = pg.rank(here());
                    final Place destination = pg.get(rank + 1 == pg.size() ? 0 : rank + 1);
                    distIdMap.forEach((Long key, String value) -> {
                        distIdMap.moveAtSync(key, destination, mm);
                    });

                    mm.sync();
                } catch (final Exception e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }

    private void z_moveToPlaceZero() throws Throwable {
        try {
            pg.broadcastFlat(() -> {
                try {
                    final CollectiveMoveManager mm = new CollectiveMoveManager(pg);
                    final Place destination = pg.get(0);
                    distIdMap.forEach((Long key, String value) -> {
                        distIdMap.moveAtSync(key, destination, mm);
                    });
                    mm.sync();
                } catch (final Exception e) {
                    System.err.println("Error on " + here());
                    e.printStackTrace();
                    throw e;
                }
            });
        } catch (final MultipleException me) {
            throw me.getSuppressed()[0];
        }
    }
}
