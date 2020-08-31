package handist.collections.dist.util;

import java.io.ObjectStreamException;
import java.io.Serializable;

import apgas.SerializableCallable;
import apgas.util.GlobalID;
import handist.collections.dist.TeamedPlaceGroup;

public class LazyObjectReference<T> implements Serializable {
	
	/** 
	 * Global ID used to identify an object with replication on multiple
	 * places
	 */
	public GlobalID globalId;
	/**
	 * Initializer called when an object replicated on multiple hosts has 
	 * not yet been allocated in a local object.
	 */
	public SerializableCallable<T> initializer;
	/**
	 * Place group on which the object of interest can be allocated on
	 */
	public TeamedPlaceGroup placeGroup;

	/** Serial Version UID */
	private static final long serialVersionUID = -968836449183221397L;

	/**
	 * Constructor
	 * @param pg place group on which the distributed object can be 
	 * manipulated
	 * @param id global identifier of the distributed object
	 * @param init initializer for the "local" instance of the distributed
	 * object when it is attempted to be accessed 
	 */
	public LazyObjectReference(TeamedPlaceGroup pg, GlobalID id, SerializableCallable<T> init) {
		globalId = id;
		placeGroup = pg;
		initializer = init;
	}

	protected Object readResolve() throws ObjectStreamException {
		Object result = globalId.getHere();
		if (result == null) {
			try {
				T r = initializer.call();
				globalId.putHereIfAbsent(r);
			} catch (Exception e) {
				throw new Error("[General Dist Manager: init should not raise exceptions.");
			}
			return globalId.getHere();
		} else {
			return result;
		}
	}
}