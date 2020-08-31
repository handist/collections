package handist.collections.dist.util;

import java.io.ObjectStreamException;
import java.io.Serializable;

import apgas.SerializableCallable;
import apgas.util.GlobalID;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.function.SerializableFunction;

public class MemberOfLazyObjectReference<T,M> extends LazyObjectReference<T> implements Serializable {
	/** Serial Version UID */
	private static final long serialVersionUID = 8438658670591463576L;
	SerializableFunction<T,M> getMember;
	
	public MemberOfLazyObjectReference(TeamedPlaceGroup pg, GlobalID id, SerializableCallable<T> initializer, SerializableFunction<T,M> memberAccess) {
		super(pg, id, initializer);
		getMember = memberAccess;
	}
	
	@Override
	protected Object readResolve() throws ObjectStreamException {
		@SuppressWarnings("unchecked")
		T t = (T) super.readResolve();
		return getMember.apply(t);
	}
}
