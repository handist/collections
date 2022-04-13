package handist.collections.dist;

import static apgas.Constructs.*;

import java.io.ObjectStreamException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import apgas.Place;
import apgas.util.GlobalID;
import handist.collections.dist.util.LazyObjectReference;
import handist.collections.dist.util.ObjectInput;
import handist.collections.dist.util.ObjectOutput;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

public class DistSortedMap<K, V> extends DistMap<K, V> implements NavigableMap<K, V>, SortedKeyRelocatable<K> {

    protected ConcurrentNavigableMap<K, V> data;

    /**
     * Construct a DistSortedMap.
     */
    public DistSortedMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistSortedMap with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistSortedMap(TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Construct a DistSortedMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     */
    public DistSortedMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        this(placeGroup, id, new ConcurrentSkipListMap<>());
    }

    /**
     * Construct a DistSortedMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     * @param data       the container to be used
     */
    protected DistSortedMap(TeamedPlaceGroup placeGroup, GlobalID id, NavigableMap<K, V> data) {
        super(placeGroup, id, data);
        this.data = (ConcurrentNavigableMap<K, V>) super.data;
        super.GLOBAL = new GlobalOperations<>(this,
                (TeamedPlaceGroup pg0, GlobalID gid) -> new DistSortedMap<>(pg0, gid));
    }

    @Override
    public Entry<K, V> ceilingEntry(K key) {
        return data.ceilingEntry(key);
    }

    @Override
    public K ceilingKey(K key) {
        return data.ceilingKey(key);
    }

    @Override
    public Comparator<? super K> comparator() {
        return data.comparator();
    }

    @Override
    public NavigableSet<K> descendingKeySet() {
        return data.descendingKeySet();
    }

    @Override
    public NavigableMap<K, V> descendingMap() {
        return data.descendingMap();
    }

    @Override
    public Entry<K, V> firstEntry() {
        return data.firstEntry();
    }

    @Override
    public K firstKey() {
        return data.firstKey();
    }

    @Override
    public Entry<K, V> floorEntry(K key) {
        return data.floorEntry(key);
    }

    @Override
    public K floorKey(K key) {
        return data.floorKey(key);
    }

    @Override
    public SortedMap<K, V> headMap(K toKey) {
        return data.headMap(toKey);
    }

    @Override
    public NavigableMap<K, V> headMap(K toKey, boolean inclusive) {
        return data.headMap(toKey, inclusive);
    }

    @Override
    public Entry<K, V> higherEntry(K key) {
        return data.higherEntry(key);
    }

    @Override
    public K higherKey(K key) {
        return data.higherKey(key);
    }

    @Override
    public Entry<K, V> lastEntry() {
        return data.lastEntry();
    }

    @Override
    public K lastKey() {
        return data.lastKey();
    }

    @Override
    public Entry<K, V> lowerEntry(K key) {
        return data.lowerEntry(key);
    }

    @Override
    public K lowerKey(K key) {
        return data.lowerKey(key);
    }

    @Override
    public void moveAtSync(K from, K to, Distribution<K> rule, MoveManager mm) {
        final ConcurrentNavigableMap<K, V> sub = data.subMap(from, to);
        if (sub.isEmpty()) {
            return;
        }
        final Iterator<K> iter = sub.keySet().iterator();
        while (iter.hasNext()) {
            final K key = iter.next();
            moveAtSync(key, rule.location(key), mm);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void moveAtSync(K from, K to, Place dest, MoveManager mm) {
        if (dest.equals(here())) {
            return;
        }
        final DistSortedMap<K, V> toBranch = this;
        final Serializer serialize = (ObjectOutput s) -> {
            final ConcurrentNavigableMap<K, V> sub = data.subMap(from, to);
            final int num = sub.size();
            s.writeInt(num);
            if (num == 0) {
                return;
            }
            final Iterator<K> iter = sub.keySet().iterator();
            while (iter.hasNext()) {
                final K key = iter.next();
                final V value = this.remove(key);
                if (value == null) {
                    throw new NullPointerException("DistSortedMap.moveAtSync null pointer value of key: " + key);
                }
                s.writeObject(key);
                s.writeObject(value);
            }
        };
        final DeSerializer deserialize = (ObjectInput ds) -> {
            final int num = ds.readInt();
            for (int i = 0; i < num; i++) {
                final K k = (K) ds.readObject();
                final V v = (V) ds.readObject();
                toBranch.putForMove(k, v);
            }
        };
        mm.request(dest, serialize, deserialize);
    }

    @Override
    public NavigableSet<K> navigableKeySet() {
        return data.navigableKeySet();
    }

    @Override
    public Entry<K, V> pollFirstEntry() {
        return data.pollFirstEntry();
    }

    @Override
    public Entry<K, V> pollLastEntry() {
        return data.pollFirstEntry();
    }

    @Override
    public NavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
        return data.subMap(fromKey, fromInclusive, toKey, toInclusive);
    }

    @Override
    public SortedMap<K, V> subMap(K fromKey, K toKey) {
        return data.subMap(fromKey, toKey);
    }

    @Override
    public SortedMap<K, V> tailMap(K fromKey) {
        return data.tailMap(fromKey);
    }

    @Override
    public NavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
        return data.tailMap(fromKey, inclusive);
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistSortedMap<>(pg1, id1);
        });
    }

}
