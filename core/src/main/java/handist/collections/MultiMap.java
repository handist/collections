package handist.collections;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;

public interface MultiMap<K, V> extends Map<K, Collection<V>> {

    /**
     * Apply the same operation onto all the local entries.
     *
     * @param op the operation.
     */
    public void forEach1(BiConsumer<K, V> op);

    /**
     * Puts a new value to the list of specified entry.
     *
     * @param key   the key of the entry
     * @param value the new value to be added to the mappings of {@code key}.
     * @return {@code true} as the collection is modified as a result (as specified
     *         by {@link Collection#add(Object)}.
     */
    public boolean put1(K key, V value);

}
