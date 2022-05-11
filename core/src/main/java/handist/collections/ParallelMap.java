package handist.collections;

import static apgas.Constructs.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;

public class ParallelMap<K, V> implements Map<K, V>, Serializable {

    private static final long serialVersionUID = 2613768150080256264L;

    protected Map<K, V> data;

    protected Function<K, V> proxyGenerator;

    public ParallelMap() {
        this(new HashMap<>());
    }

    protected ParallelMap(Map<K, V> data) {
        this.data = data;
    }

    /**
     * Remove the all local entries.
     */
    @Override
    public void clear() {
        data.clear();
    }

    /**
     * Return true if the specified entry is exist in the local collection.
     *
     * @param key a key.
     * @return true is the specified object is a key present in the local map,
     */
    @Override
    public boolean containsKey(Object key) {
        return data.containsKey(key);
    }

    /**
     * Indicates if the provided value is contained in the local map.
     */
    @Override
    public boolean containsValue(Object value) {
        return data.containsValue(value);
    }

    boolean debugPrint() {
        return true;
    }

    /**
     * Removes the provided key from the local map, returns {@code true} if there
     * was a previous obejct mapped to this key, {@code false} if there were no
     * mapping with this key or if the mapping was a {@code null} object
     *
     * @param key the key to remove from this local map
     * @return true if a mapping was removed as a result of this operation, false
     *         otherwise
     */
    public boolean delete(K key) {
        final V result = data.remove(key);
        return (result != null);
    }

    /**
     * Return the Set of local entries.
     *
     * @return the Set of local entries.
     */
    @Override
    public Set<Entry<K, V>> entrySet() {
        return data.entrySet();
    }

    /**
     * Apply the specified operation with each Key/Value pair contained in the local
     * collection.
     *
     * @param action the operation to perform
     */
    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        if (!data.isEmpty()) {
            data.forEach(action);
        }
    }

    public void forEach(SerializableConsumer<V> action) {
        for (final Entry<K, V> entry : data.entrySet()) {
            action.accept(entry.getValue());
        }
    }

    private void forEachParallelBodyLocal(SerializableConsumer<V> action) {
        final List<Collection<V>> separated = separateLocalValues(Runtime.getRuntime().availableProcessors() * 2);
        for (final Collection<V> sub : separated) {
            async(() -> {
                sub.forEach(action);
            });
        }
    }

    /**
     * Helper method which separates the keys contained in the local map into even
     * batches for the number of threads available on the system and applies the
     * provided action on each key/value pair contained in the collection in
     * parallel
     *
     * @param action action to perform on the key/value pair contained in the map
     */
    private void forEachParallelKey(SerializableBiConsumer<? super K, ? super V> action) {
        final int batches = Runtime.getRuntime().availableProcessors();

        // Dispatch the existing keys into batches
        final List<Collection<K>> keys = new ArrayList<>(batches);
        for (int i = 0; i < batches; i++) {
            keys.add(new HashSet<>());
        }
        // Round-robin of keys into batches
        int i = 0;
        for (final K k : data.keySet()) {
            keys.get(i++).add(k);
            if (i >= batches) {
                i = 0;
            }
        }

        // Spawn asynchronous activity for each batch
        for (final Collection<K> keysToProcess : keys) {
            async(() -> {
                // Apply the supplied action on each key in the batch
                for (final K key : keysToProcess) {
                    final V value = data.get(key);
                    action.accept(key, value);
                }
            });
        }
    }

    /**
     * Return the element for the provided key. If there is no element at the index,
     * return null.
     *
     * When an agent generator is set on this instance and there is no element at
     * the index, a proxy value for the index is generated as a return value.
     *
     * @param key the index of the value to retrieve
     * @return the element associated with {@code key}, or null if this map contains
     *         no mapping for the key
     */
    @SuppressWarnings("unchecked")
    @Override
    public V get(Object key) {
        final V result = data.get(key);
        if (result != null) {
            return result;
        }
        if (proxyGenerator != null && !data.containsKey(key)) {
            return proxyGenerator.apply((K) key);
        } else {
            return null;
        }
    }

    /**
     * Indicates if the local distributed map is empty or not
     *
     * @return {@code true} if there are no mappings in the local map
     */
    @Override
    public boolean isEmpty() {
        return data.isEmpty();
    }

    /**
     * Return the Set of local keys.
     *
     * @return the Set of local keys.
     */
    @Override
    public Set<K> keySet() {
        return data.keySet();
    }

    /**
     * Apply the same operation on the all elements including remote places and
     * creates a new {@link ParallelMap} with the same keys as this instance and the
     * result of the mapping operation as values.
     *
     * @param <W> result type of mapping operation
     * @param op  the map operation from type <code>V</code> to <code>W</code>
     * @return a DistMap from <code>K</code> to <code>W</code> built from applying
     *         the mapping operation on each element of this instance
     */
    public <W> ParallelMap<K, W> map(Function<V, W> op) {
        throw new Error("not supported yet");
        // TODO
        /*
         * return new ParallelMap<T,S>(placeGroup, team, () -> { val dst = new
         * HashMap<T,S>(); for (entry in entries()) { val key = entry.getKey(); val
         * value = entry.getValue(); dst(key) = op(value); } return dst; });
         */
    }

    /**
     * Parallel version of {@link #forEach(BiConsumer)}
     *
     * @param action the action to perform on every key/value pair contained in this
     *               local map
     */
    public void parallelForEach(SerializableBiConsumer<? super K, ? super V> action) {
        finish(() -> {
            forEachParallelKey(action);
        });
    }

    public void parallelForEach(SerializableConsumer<V> action) {
        parallelForEachLocal(action);
    }

    private void parallelForEachLocal(SerializableConsumer<V> action) {
        finish(() -> {
            forEachParallelBodyLocal(action);
        });
    }

    void printLocalData() {
        System.out.println(this);
    }

    /**
     * Put a new entry.
     *
     * @param key   the key of the new entry.
     * @param value the value of the new entry.
     * @return the previous value associated with {@code key}, or {@code null} if
     *         there was no mapping for {@code key}.(A {@code null} return can also
     *         indicate that the map previously associated {@code null} with
     *         {@code key}.)
     */
    @Override
    public V put(K key, V value) {
        return data.put(key, value);
    }

    /**
     * Adds all the mappings contained in the specified map into this local map.
     */
    @Override
    public void putAll(java.util.Map<? extends K, ? extends V> m) {
        data.putAll(m);
    }

    /**
     * Reduce the all local elements using the given operation.
     *
     * @param <S>  type of the result produced by the reduction operation
     * @param op   the operation used in the reduction
     * @param unit the neutral element of the reduction operation
     * @return the result of the reduction
     */
    public <S> S reduceLocal(BiFunction<S, V, S> op, S unit) {
        S accum = unit;
        for (final Map.Entry<K, V> entry : data.entrySet()) {
            accum = op.apply(accum, entry.getValue());
        }
        return accum;
    }

    /**
     * Remove the entry corresponding to the specified key in the local map.
     *
     * @param key the key corresponding to the value.
     * @return the previous value associated with the key, or {@code null} if there
     *         was no existing mapping (or the key was mapped to {@code null})
     */
    @Override
    public V remove(Object key) {
        return data.remove(key);
    }

    private List<Collection<V>> separateLocalValues(int n) {
        final List<Collection<V>> result = new ArrayList<>(n);
        final long totalNum = size();
        final long rem = totalNum % n;
        final long quo = totalNum / n;
        if (data.isEmpty()) {
            return result;
        }
        final Iterator<V> it = data.values().iterator();
        List<V> list = new ArrayList<>();
        for (long i = 0; i < n; i++) {
            list = new ArrayList<>();
            final long count = quo + ((i < rem) ? 1 : 0);
            for (long j = 0; j < count; j++) {
                if (it.hasNext()) {
                    list.add(it.next());
                }
            }
            result.add(list);
        }
        return result;
    }

    /**
     * Sets the proxy generator for this instance.
     * <p>
     * The proxy will be used to generate values when accesses to a key not
     * contained in this instance is made. Instead of throwing an exception, the
     * proxy will be called with the attempted index and the program will continue
     * with the value returned by the proxy.
     * <p>
     * This feature is similar to {@link Map#getOrDefault(Object, Object)}
     * operation, the difference being that instead of returning a predetermined
     * default value, the provided function is called with the key.
     *
     * @param proxy function which takes a key "K" as parameter and returns a "V",
     *              or {@code null} to remove any previously set proxy
     */
    public void setProxyGenerator(Function<K, V> proxy) {
        proxyGenerator = proxy;
    }

    /**
     * Return the number of local entries.
     *
     * @return the number of the local entries.
     */
    @Override
    public int size() {
        return data.size();
    }

    /**
     * Returns all the values of this local map in a collection.
     */
    @Override
    public Collection<V> values() {
        return data.values();
    }

}
