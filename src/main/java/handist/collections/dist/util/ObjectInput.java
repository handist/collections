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

import java.io.InputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import apgas.impl.KryoSerializer;

public class ObjectInput {

    final Input input;
    boolean isClosed = false;
    final Kryo kryo;

    final InputStream stream;

    public ObjectInput(InputStream in) {
        if (in == null) {
            throw new NullPointerException();
        }
        stream = in;
        input = new Input(stream);
        kryo = KryoSerializer.kryoThreadLocal.get();
        kryo.reset();
        kryo.setAutoReset(false);
    }

    public void close() {
        input.close();
        kryo.reset();
        kryo.setAutoReset(true); // Need for using at remote place.
        isClosed = true;
    }

    public byte readByte() {
        if (isClosed) {
            throw new RuntimeException(this + " has closed.");
        }
        return input.readByte();
    }

    public int readInt() {
        if (isClosed) {
            throw new RuntimeException(this + " has closed.");
        }
        return input.readInt();
    }

    public long readLong() {
        if (isClosed) {
            throw new RuntimeException(this + " has closed.");
        }
        return input.readLong();
    }

    public Object readObject() {
        if (isClosed) {
            throw new RuntimeException(this + " has closed.");
        }
        return kryo.readClassAndObject(input);
    }

    public void setAutoReset(boolean autoReset) {
        if (isClosed) {
            return;
        }
        kryo.setAutoReset(autoReset);
    }
}
