package handist.collections.function;

import java.io.Serializable;
import java.util.function.Function;

public interface SerializableFunction<T1, T2> extends Function<T1, T2>, Serializable {

}
