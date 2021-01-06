package handist.collections.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Serializable function
 * @author Patrick
 *
 * @param <T1> input type of the function
 * @param <T2> return type of the function
 */
public interface SerializableFunction<T1, T2> extends Function<T1, T2>, Serializable {
}
