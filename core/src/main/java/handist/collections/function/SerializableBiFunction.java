package handist.collections.function;

import java.io.Serializable;
import java.util.function.BiFunction;

public interface SerializableBiFunction<A, B, C> extends BiFunction<A, B, C>, Serializable {
}
