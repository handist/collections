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
package handist.collections.glb.lifeline;

import java.lang.reflect.Constructor;

import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.glb.Config;

/**
 * Class in charge of performing the reflection operations necessary to
 * instanciate lifeline instance.
 *
 * @author Patrick Finnerty
 *
 */
public class LifelineFactory {
    /**
     * Constructor used to produce new Lifeline instances when the need arises.
     */
    private static Constructor<Lifeline> constructor = null;

    /**
     * Factory method returning a new instance of the lifeline implementation
     * specified through property settings.
     *
     * @param pg the place group among which a lifeline network needs to be
     *           established
     * @return a new Lifeline implementation instance prepared for the specified
     *         group of laces
     * @throws Exception if thrown while making a reflection operation
     */
    @SuppressWarnings("unchecked")
    public static Lifeline newLifeline(TeamedPlaceGroup pg) throws Exception {
        if (constructor == null) {
            @SuppressWarnings("rawtypes")
            final Class lifelineClass = Class.forName(Config.getLifelineClassName());
            constructor = lifelineClass.getConstructor(TeamedPlaceGroup.class);
        }

        return constructor.newInstance(pg);
    }

    /**
     * Facotry method returning a new instance of the lifeline class specified as
     * parameter and the designated {@link TeamedPlaceGroup}.
     *
     * @param pg            the group of places on which the collection is defined
     * @param lifelineClass the class to be used as lifeline implementation
     * @return a constructed lifeline instance
     * @throws Exception if thrown while making reflective operation
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Lifeline newLifeline(TeamedPlaceGroup pg, Class lifelineClass) throws Exception {
        final Constructor<Lifeline> classConstructor = lifelineClass.getConstructor(TeamedPlaceGroup.class);
        return classConstructor.newInstance(pg);
    }

}
