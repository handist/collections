package handist.collections.dist;

import java.io.ObjectStreamException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.BiConsumer;

import apgas.util.GlobalID;
import handist.collections.MultiMap;
import handist.collections.dist.util.LazyObjectReference;
import mpjbuf.IllegalArgumentException;

public class DistSortedMultiMap<K, V> extends DistSortedMap<K, Collection<V>> implements MultiMap<K, V> {

    /**
     * Construct a DistSortedMultiMap.
     */
    public DistSortedMultiMap() {
        this(TeamedPlaceGroup.getWorld());
    }

    /**
     * Construct a DistSortedMultiMap with given argument.
     *
     * @param placeGroup PlaceGroup.
     */
    public DistSortedMultiMap(TeamedPlaceGroup placeGroup) {
        this(placeGroup, new GlobalID());
    }

    /**
     * Construct a DistSortedMultiMap with given arguments.
     *
     * @param placeGroup PlaceGroup
     * @param id         the global ID used to identify this instance
     */
    public DistSortedMultiMap(TeamedPlaceGroup placeGroup, GlobalID id) {
        super(placeGroup, id);
        super.GLOBAL = new GlobalOperations<>(this,
                (TeamedPlaceGroup pg0, GlobalID gid) -> new DistSortedMultiMap<>(pg0, gid));
    }

    /**
     * create an empty collection to hold values for a key. Please define the
     * adequate container for the class.
     *
     * @return the created collection.
     */
    protected Collection<V> createEmptyCollection() {
        return new ArrayList<>();
    }

    @Override
    public void forEach1(BiConsumer<K, V> op) {
        for (final Entry<K, Collection<V>> entry : data.entrySet()) {
            final K key = entry.getKey();
            for (final V value : entry.getValue()) {
                op.accept(key, value);
            }
        }
    }

    /**
     * Return new {@link MultiMapEntryDispatcher} instance that enable fast
     * relocation between places than normal.
     *
     * @param rule Determines the dispatch destination.
     * @return :
     */
    @Override
    public MultiMapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule) {
        return new MultiMapEntryDispatcher<>(this, placeGroup(), rule);
    }

    /**
     * Return new {@link MapEntryDispatcher} instance that enable fast relocation
     * between places than normal.
     *
     * @param rule Determines the dispatch destination.
     * @param pg   Relocate in this placegroup.
     * @return :
     * @throws IllegalArgumentException :
     */
    @Override
    public MultiMapEntryDispatcher<K, V> getObjectDispatcher(Distribution<K> rule, TeamedPlaceGroup pg)
            throws IllegalArgumentException {
        return new MultiMapEntryDispatcher<>(this, pg, rule);
    }

    @Override
    public Collection<V> put(K key, Collection<V> values) {
        Collection<V> list = data.get(key);
        if (list == null) {
            list = createEmptyCollection();
            data.put(key, list);
        }
        if (values != null) {
            list.addAll(values);
        }
        return list;
    }

    @Override
    public boolean put1(K key, V value) {
        Collection<V> list = data.get(key);
        if (list == null) {
            list = createEmptyCollection();
            data.put(key, list);
        }
        return list.add(value);
    }

    @Override
    public Collection<V> putForMove(K key, Collection<V> values) {
        Collection<V> list = data.get(key);
        if (list == null) {
            list = createEmptyCollection();
            data.put(key, list);
        }
        // TODO we should check values!=null before transportation
        if (values != null) {
            list.addAll(values);
        }
        return list;
    }

    @Override
    public Object writeReplace() throws ObjectStreamException {
        final TeamedPlaceGroup pg1 = placeGroup;
        final GlobalID id1 = id;
        return new LazyObjectReference<>(pg1, id1, () -> {
            return new DistSortedMultiMap<>(pg1, id1);
        });
    }

}
