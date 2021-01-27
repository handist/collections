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
package handist.collections.glb;

import java.util.Random;

import apgas.Constructs;
import apgas.MultipleException;
import handist.collections.function.SerializableConsumer;

/**
 * Utility class which gathers parts of test routines useful to all tests
 *
 * @author Patrick
 *
 */
public class Util {

    public final static SerializableConsumer<Element> addZToPrefix = (e) -> e.s = "Z" + e.s;

    public final static SerializableConsumer<Element> makePrefixTest = (e) -> e.s = "Test" + e.s;
    public final static SerializableConsumer<Element> makeSuffixTest = (e) -> e.s = e.s + "Test";
    /**
     * Initialize a random object to help generate dummy values to populate the
     * collections under test
     */
    static Random random = new Random(Constructs.here().id);

    /**
     * Helper method to generate Strings with the provided prefix.
     *
     * @param prefix the String prefix of the Random string generated
     * @return a random String with the provided prefix
     */
    public static String genRandStr(String prefix) {
        final long rndLong = random.nextLong();
        return prefix + rndLong;
    }

    public static void printExceptionAndThrowFirst(MultipleException me) throws Throwable {
        me.printStackTrace();
        Throwable t = me;
        while (t instanceof MultipleException) {
            t = ((MultipleException) t).getSuppressed()[0];
        }
        System.err.println("First non-MultipleException  error:");
        t.printStackTrace();
        throw t;
    }
}
