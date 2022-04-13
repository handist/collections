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
import handist.collections.function.SerializableFunction;

/**
 * Variation on {@link LazyObjectReference} which allows us to serialize an
 * object which is a member of a {@link LazyObjectReference}.
 *
 * @param <T> type of the object used a lazy reference
 * @param <M> type of the member being serialized
 * @see LazyObjectReference
 */
public class MemberOfLazyObjectReference<T, M> extends LazyObjectReference<T> implements Serializable {
    /** Serial Version UID */
    private static final long serialVersionUID = 8438658670591463576L;

    /**
     * Function which retrieves the member of the serialized object after the object
     * has been initialized on the remote host
     */
    SerializableFunction<T, M> getMember;

    /**
     * Constructor
     *
     * @param pg           place group on which this object may be serialized and
     *                     send to
     * @param id           global id used to identify the various instances of the
     *                     parent lazy object reference
     * @param initializer  initializer for object T
     * @param memberAccess function taking am object T as parameter and returning
     *                     the member M of that object
     */
    public MemberOfLazyObjectReference(TeamedPlaceGroup pg, GlobalID id, SerializableCallable<T> initializer,
            SerializableFunction<T, M> memberAccess) {
        super(pg, id, initializer);
        getMember = memberAccess;
    }

    @Override
    protected Object readResolve() throws ObjectStreamException {
        @SuppressWarnings("unchecked")
        final T t = (T) super.readResolve();
        return getMember.apply(t);
    }
}
