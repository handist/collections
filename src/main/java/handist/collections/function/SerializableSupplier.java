package handist.collections.function;

import java.io.Serializable;
import java.util.function.Supplier;

/**
 * Serializable implementation of the standard Java {@link Supplier} interface.
 * @author Patrick
 *
 * @param <T> type provided by this serializable supplier
 */
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
