package handist.collections.glb;

import java.io.Serializable;

import handist.collections.Chunk;
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
	final DistFuture<DistCol<T>> future = new DistFuture<>(col);

	final SerializableSupplier<GlbTask> initGlbTask = () -> {
	    return new DistColGlbTask(col);
	};

	// We transform the action to accept a LongRange as parameter, retrieve
	// the T at the each index, and apply the lambda given as parameter to these Ts
	final SerializableConsumer<LongRange> realAction = (lr) -> {
	    for (long l = lr.from; l < lr.to; l++) {
		action.accept(col.get(l));
	    }
	};

	// Create the operation with all the types/arguments
	final GlbOperation<DistCol<T>, T, LongRange, LongRange, DistCol<T>> operation = new GlbOperation<>(col,
		realAction, future, initGlbTask);
	// Submit the operation to the GLB
	glb.submit(operation);

	// return the future to the programmer
	return future;
    }

    /**
     * GLB operation which creates a new {@link DistCol} using the mapping operation
     * provided as parameter. The resulting {@link DistCol} will contain the same
     * indices as this collection. The value stored at each index of the resulting
     * collection will be the result of the provided mapping operation for this
     * collection at the same index. As part of the GLB consists in moving entries
     * from place to place, it is possible for the distribution of the resulting
     * collection and this collection to differ.
     *
     * @param <U> type of the result of the map function provided as parameter
     * @param map function which takes an object T as input and returns a instance
     *            of type U
     * @return a {@link DistFuture}
     */
    public <U> DistFuture<DistCol<U>> map(SerializableFunction<T, U> map) {
	final GlobalLoadBalancer glb = getGlb();

	// Create new collection to contain the result
	final DistCol<U> resultCollection = new DistCol<>(col.placeGroup());

	// Adapt the provided map to represent what the glb workers will actually
	// perform
	final SerializableConsumer<LongRange> realAction = (lr) -> {
	    // First, initialize a Chunk to place the mappings
	    final Chunk<U> c = new Chunk<>(lr);

	    /*
	     * FIXME ChunkedList (parent of DistCol) does not support concurrent insertion
	     * of chunks As a result, inserting a chunk in the result collection must be
	     * done in mutual exclusion with any other worker inserting chunks in this
	     * collection on the local host
	     */
	    synchronized (resultCollection) {
		resultCollection.add(c);
	    }

	    // Iterate on the elements
	    for (long l = lr.from; l < lr.to; l++) {
		final T t = col.get(l);
		final U u = map.apply(t);
		c.set(l, u);
	    }
	};

	// Initialize the future returned to the programmer in the underGLB method
	// In this operation, the collection involved is the handle itself
	final DistFuture<DistCol<U>> future = new DistFuture<>(resultCollection);

	final SerializableSupplier<GlbTask> initGlbTask = () -> {
	    return new DistColGlbTask(col);
	};

	// Create the operation with all the types/arguments
	final GlbOperation<DistCol<T>, T, LongRange, LongRange, DistCol<U>> operation = new GlbOperation<>(col,
		realAction, future, initGlbTask);

	// Submit the operation to the GLB
	glb.submit(operation);

	// return the future to the programmer
	return future;
    }
}
