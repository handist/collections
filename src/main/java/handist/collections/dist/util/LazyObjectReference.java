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
package handist.collections.dist.util;

import java.io.ObjectStreamException;
import java.io.Serializable;

import apgas.SerializableCallable;
import apgas.util.GlobalID;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Class used as a substitute when serializing an object. This class allows us
 * to delay the initialization of an object on a remote place until an
 * asynchronous remote activity is actually executed on this remote host.
 *
 * @param <T> type of the object being referenced
 * @see MemberOfLazyObjectReference
 */
public class LazyObjectReference<T> implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -968836449183221397L;
    /**
     * Global ID used to identify an object with replication on multiple places
     */
    public GlobalID globalId;
    /**
     * Initializer called when an object replicated on multiple hosts has not yet
     * been allocated in a local object.
     */
    public SerializableCallable<T> initializer;

    /**
     * Place group on which the object of interest can be allocated on
     */
    public TeamedPlaceGroup placeGroup;

    /**
     * Constructor
     *
     * @param pg   place group on which the distributed object can be manipulated
     * @param id   global identifier of the distributed object
     * @param init initializer for the "local" instance of the distributed object
     *             when it is attempted to be accessed
     */
    public LazyObjectReference(TeamedPlaceGroup pg, GlobalID id, SerializableCallable<T> init) {
        globalId = id;
        placeGroup = pg;
        initializer = init;
    }

    protected Object readResolve() throws ObjectStreamException {
        final Object result = globalId.getHere();
        if (result == null) {
            try {
                final T r = initializer.call();
                globalId.putHereIfAbsent(r);
            } catch (final Exception e) {
                throw new Error("[General Dist Manager: init should not raise exceptions.");
            }
            return globalId.getHere();
        } else {
            return result;
        }
    }
}
