package handist.collections.dist;

import java.util.Collection;
import java.util.Map.Entry;

import apgas.Place;

/**
 * Interface defining a number of methods allowing the transfer of data objects 
 * between the local containers of a distributed collection. 
 * 
 * @param <R> Type of the range used to identify multiple objects
 */
public interface RangeRelocatable<R> {

	/**
	 * Obtain all the ranges on which this instance has mappings
	 * @return collection of ranges
	 */
	public Collection<R> getAllRanges();
	
	/**
	 * Marks the objects in the specified range for transfer to the specified 
	 * destination. The transfer will be performed when the 
	 * {@link MoveManagerLocal#sync()} is called.
	 * @param range the range of objects to transfer
	 * @param destination the destination of the objects
	 * @param manager the instance in charge of handling a batch of object tranfers
	 */
	public void moveRangeAtSync(R range, Place destination, MoveManagerLocal manager);
	
	/**
	 * Marks all the objects contained in all the ranges for transfer to the 
	 * specified destination. The transfer will be performed the next tim the 
	 * provided manager's {@link MoveManagerLocal#sync()} method is called.
	 * <p>
	 * The default implementation consists in repetitively calling 
	 * {@link #moveRangeAtSync(Object, Place, MoveManagerLocal)} for each individual 
	 * range. Implementations are free to use a more efficient design.
	 * @param ranges collections of ranges that describe objects to transfer
	 * @param destination the place to which the objects should be transfered
	 * @param manager the manager in charge of performing this batch of transfers
	 */
	public default void moveRangeAtSync(Collection<R> ranges, Place destination, MoveManagerLocal manager) {
		for (R range : ranges) {
			moveRangeAtSync(range, destination, manager);
		}
	}
	
	/**
	 * Marks the elements in the provided range for transfer using the specified
	 * {@link RangedDistribution}. The provided range may be split into multiple
	 * sub-ranges to be transfered to different destinations. The actual 
	 * transfer will be performed when the {@link MoveManagerLocal#sync()} method
	 * is called.  
	 * @param range the range of entries to transfer
	 * @param destination function creating a map from ranges to Place
	 * @param manager the manager in charge of the transfer
	 */
	public default void moveRangeAtSync(R range, RangedDistribution<R> destination, MoveManagerLocal manager ) {
		for (Entry<R, Place> mappings : destination.placeRanges(range).entrySet()) {
			moveRangeAtSync(mappings.getKey(), mappings.getValue(), manager);
		}
	}
	
	/**
	 * Marks all the elements in the provided ranges for transfer using the 
	 * specified {@link RangedDistribution}. The provided ranges may be split 
	 * into multiple sub-ranges to be transfered to different destinations. The
	 * actual transfer will be performed when the {@link MoveManagerLocal#sync()}
	 * method of the provided manager is called
	 * @param ranges ranges considered for transfer
	 * @param destination function which given a range will indicate where the 
	 * elements of this range should be relocated.
	 * @param manager manager in charge of performing the object transfer
	 */
	public default void moveRangeAtSync(Collection<R> ranges, RangedDistribution<R> destination, MoveManagerLocal manager) {
		for (R range : ranges) {
			moveRangeAtSync(range, destination, manager);
		}
	}
}
