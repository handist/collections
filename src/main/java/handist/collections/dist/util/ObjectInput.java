package handist.collections.dist.util;

import java.io.InputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import apgas.impl.KryoSerializer;

public class ObjectInput {
	
	final InputStream stream;
	final Input input;
	final Kryo kryo;
	
	boolean isClosed = false;
	
	
	public ObjectInput(InputStream in) {
		if(in == null) {
			throw new NullPointerException();
		}
		stream = in;
		input = new Input(stream);
		kryo = KryoSerializer.kryoThreadLocal.get();
		kryo.reset();		
		kryo.setAutoReset(false);
	}
	
	public Object readObject() {		
		if(isClosed)
			throw new RuntimeException(this + " has closed.");
		return kryo.readClassAndObject(input);				
	}
	
	public int readInt() {		
		if(isClosed)
			throw new RuntimeException(this + " has closed.");
		return input.readInt();
	}
	
	public long readLong() {	
		if(isClosed)
			throw new RuntimeException(this + " has closed.");
		return input.readLong();
	}
	
	public byte readByte() {	
		if(isClosed)
			throw new RuntimeException(this + " has closed.");
		return input.readByte();
	}
		
	public void close() {		
		input.close();
		kryo.reset();
		kryo.setAutoReset(true);	// Need for using at remote place.
		isClosed = true;
	}
	
	public void setAutoReset(boolean autoReset) {
		if(isClosed)
			return;
		kryo.setAutoReset(autoReset);
	}
}
