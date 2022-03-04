package handist.collections.dist;

import apgas.Place;

public interface SortedKeyRelocatable<K> {

    public void moveAtSync(K from, K to, Distribution<K> rule, MoveManager mm);

    public void moveAtSync(K from, K to, Place destination, MoveManager mm);
}
