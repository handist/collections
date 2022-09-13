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

import java.io.ByteArrayOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import apgas.impl.KryoSerializer;

public class ObjectOutput {

    private int count;
    final Kryo kryo;
    final Output output;
    final ByteArrayOutputStream stream;

    public ObjectOutput(ByteArrayOutputStream out) {
        this(out, true);
    }

    public ObjectOutput(ByteArrayOutputStream out, boolean references) {
        if (out == null) {
            throw new NullPointerException();
        }
        stream = out;
        output = new Output(stream);
        kryo = KryoSerializer.getKryoInstance();
        kryo.setAutoReset(false);
        kryo.setReferences(references);
        count = 0;
    }

    public void clear() {
        kryo.reset();
        stream.reset();
        count = 0;
    }

    public void close() {
        output.close();
        kryo.reset();
    }

    public void flush() {
        output.flush();
    }

    public int getCount() {
        return count;
    }

    public void reset() {
        kryo.reset();
    }

    public byte[] toByteArray() {
        return stream.toByteArray();
    }

    public void writeByte(byte val) {
        output.writeByte(val);
        count++;
    }

    public void writeDouble(double val) {
        output.writeDouble(val);
        count++;
    }

    public void writeInt(int val) {
        output.writeInt(val);
        count++;
    }

    public void writeLong(long val) {
        output.writeLong(val);
        count++;
    }

    public void writeObject(Object obj) {
        kryo.writeClassAndObject(output, obj);
        count++;
    }
}
