package handist.collections.glb;

import java.io.Serializable;

import handist.collections.LongRange;
import handist.collections.dist.DistCol;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.SerializableFunction;
import handist.collections.function.SerializableSupplier;

/**
 * This class proposes various operations that operate on all the elements of a
 * {@link DistCol} as part of a GLB program. Any call to methods of this class
 * should be made from within a
 * {@link GlobalLoadBalancer#underGLB(apgas.SerializableJob)} method.
 * 
 * @author Patrick Finnerty
 * @param <T> type of the elements contained in the underlying distributed
 *            collection
 */
public class DistColGlb<T> extends AbstractGlbHandle implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = 612021438330155918L;

	/** Underlying collection on which the operations of this class operate */
	DistCol<T> col;

	/**
	 * Constructor
	 * 
	 * @param c collection on which this handle will operate
	 */
	public DistColGlb(DistCol<T> c) {
		col = c;
	}

	/**
	 * Applies the specified action to all the elements contained in the
	 * {@link DistCol} and returns the underlying collection
	 * 
	 * @param action action to perform on each element
	 * @return future representing this "forEach" operation which will return the
	 *         underlying {@link DistCol} collection upon termination
	 */
	public DistFuture<DistCol<T>> forEach(SerializableConsumer<T> action) {
		final GlobalLoadBalancer glb = getGlb();

		// Initialize the future returned to the programmer in the underGLB method
		// In this operation, the collection involved is the handle itself
		DistFuture<DistCol<T>> future = new DistFuture<>(col);

		SerializableSupplier<GlbTask> initGlbTask = () -> {
			return new DistColGlbTask(col);
		};

		// We transform the action to accept a long index as parameter, retrieve 
		// the T at the specified index, and apply the lambda given as parameter
		final SerializableConsumer<Long> realAction = (l)->action.accept(col.get(l));


		// Create the operation with all the types/arguments
		final GlbOperation<DistCol<T>, T, Long,LongRange, DistCol<T>> operation = new GlbOperation<DistCol<T>, T, Long, LongRange, DistCol<T>>(
				col, 
				realAction, 
				future, 
				initGlbTask);

		// Submit the operation to the GLB
		glb.submit(operation);

		// return the future to the programmer
		return future;
	}

	/**
	 * GLB operation which creates a new {@link DistCol} using the mapping operation provided as parameter. 
	 * The resulting {@link DistCol} will contain the same indices as this collection. The value stored at 
	 * each index of the resulting collection will be the result of the provided mapping operation for this 
	 * collection at the same index. As part of the GLB consists in moving entries from place to place, it 
	 * is possible for the distribution of the resulting collection and this collection to differ. 
	 * @param <U> type of the result of the map function provided as parameter
	 * @param map function which takes an object T as input and returns a instance of type U
	 * @return a {@link DistFuture} 
	 */
	/*
	 * FIXME this method compiles. Yet it will not work: we need to initialize the various chunks in which 
	 * values will be placed thanks to the map action. 
	 */
	public <U> DistFuture<DistCol<U>> map(SerializableFunction<T,U> map) {
		final GlobalLoadBalancer glb = getGlb();

		// Create new collection to contain the result
		final DistCol<U> resultCollection = new DistCol<>(col.placeGroup());

		// Adapt the provided map to represent what the glb workers will actually
		// perform
		SerializableConsumer<Long> realAction = (l)->{
			T t = col.get(l);
			U u = map.apply(t);
			resultCollection.set(l, u);
		};

		// Initialize the future returned to the programmer in the underGLB method
		// In this operation, the collection involved is the handle itself
		DistFuture<DistCol<U>> future = new DistFuture<>(resultCollection);

		SerializableSupplier<GlbTask> initGlbTask = () -> {
			return new DistColGlbTask(col);
		};

		// Create the operation with all the types/arguments
		final GlbOperation<DistCol<T>, T, Long, LongRange, DistCol<U>> operation = new GlbOperation<DistCol<T>, T, Long, LongRange, DistCol<U>>(
				col, 
				realAction, 
				future, 
				initGlbTask);

		// Submit the operation to the GLB
		glb.submit(operation);

		// return the future to the programmer
		return future;
	}
}
