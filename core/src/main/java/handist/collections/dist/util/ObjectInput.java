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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import apgas.impl.KryoSerializer;

public class ObjectInput {

    final Input input;
    final Kryo kryo;

    final ByteArrayInputStream stream;

    public ObjectInput(ByteArrayInputStream in) {
        this(in, true);
    }

    public ObjectInput(ByteArrayInputStream in, boolean references) {
        if (in == null) {
            throw new NullPointerException();
        }
        stream = in;
        input = new Input(stream);
        kryo = KryoSerializer.getKryoInstance();
        kryo.setAutoReset(false);
        kryo.setReferences(references);
    }

    public int available() throws IOException {
        return input.available();
    }

    public void close() {
        input.close();
        kryo.reset();
    }

    public byte readByte() {
        return input.readByte();
    }

    public double readDouble() {
        return input.readDouble();
    }

    public int readInt() {
        return input.readInt();
    }

    public long readLong() {
        return input.readLong();
    }

    public Object readObject() {
        return kryo.readClassAndObject(input);
    }

    public void reset() {
        kryo.reset();
    }

    public void setAutoReset(boolean autoReset) {
        kryo.setAutoReset(autoReset);
    }
}
