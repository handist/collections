package handist.collections.dist;

import java.util.Collection;

import apgas.Place;

/**
 * Interface defining a number of methods allowing transfer of entries between 
 * the local containers of a distributed collection.
 *
 * @param <K> Type of the key / identifier that represent a transferable object
 * instance
 */
public interface KeyRelocatable<K> {
	
	/**
	 * Obtain all the keys contained in the local handle of the distributed 
	 * collection. 
	 * @return all the keys that are susceptible to be moved to other places in
	 * a collection
	 */
	public Collection<K> getAllKeys();

	/**
	 * Marks the specified key for relocation over to the specified place. The
	 * transfer will be performed the next time the manager's 
	 * {@link MoveManagerLocal#sync()} method is called.
	 * @param key key to mark for relocation
	 * @param destination place on which the key needs to be transfered
	 * @param manager manager in charge of the transfer
	 */
	public void moveAtSync(K key, Place destination, MoveManagerLocal manager);
	
	/**
	 * Marks all the keys in the provided collection for relocation over to the
	 * specified place. The actual transfer will be performed the next time the
	 * specified manager's {@link MoveManagerLocal#sync()} method is called.
	 * @param keys collection of keys to be relocated
	 * @param destination the place to which these keys should be relocated
	 * @param manager the manager in charge of performing the relocation
	 */
	public default void moveAtSync(Collection<K> keys, Place destination, MoveManagerLocal manager) {
		for (K key : keys) {
			moveAtSync(key, destination, manager);
		}
	}
	
	/**
	 * Marks the keys in the specified collection for transfer according to the
	 * distribution provided as parameter. The transfer will be effective when 
	 * the manager's {@link MoveManagerLocal#sync()} method is called.
	 * @param <D> type representing a function from K to Place
	 * @param keys the keys to be marked for transfer
	 * @param distribution the distribution which indicates the destination of 
	 * each individual key
	 * @param manager the move manager in charge of the transfer
	 */
	public default <D extends Distribution<K>> void moveAtSync(Collection<K> keys, D distribution, MoveManagerLocal manager) {
		for (K key : keys) {
			moveAtSync(key, distribution.place(key), manager);
		}
	}
	
	/**
	 * Marks all the keys of this local handle for relocation using the provided
	 * distribution to determine where each individual keys should go. The 
	 * transfer is actually performed the next the specified manager's 
	 * {@link MoveManagerLocal#sync()} method is called.
	 * @param distribution the function that determines where each individual 
	 * key should be relocated to
	 * @param manager the move manager in charge of the transfer
	 */
	public default void moveAtSync(Distribution<K> distribution, MoveManagerLocal manager) {
		moveAtSync(getAllKeys(), distribution, manager);
	}
}