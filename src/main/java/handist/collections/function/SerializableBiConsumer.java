package handist.collections.function;

import java.io.Serializable;
import java.util.function.BiConsumer;

/**
 * Serializable BiConsumer function. This interface used to represent 
 * {@link BiConsumer}s that need to be serialized and transmitted to remote 
 * hosts to be executed.  
 *
 * @param <T> type of the first parameter
 * @param <U> type of the second parameter
 * @see SerializableConsumer
 */
public interface SerializableBiConsumer<T, U> extends BiConsumer<T, U>, Serializable {
}
