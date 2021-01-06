package handist.collections.glb;

import java.io.Serializable;

import javax.naming.OperationNotSupportedException;

/**
 * DistFuture represents the progress of an operation that can take place under GLB
 * @author Patrick Finnerty
 * @param <R> type of the distributed collection returned by the operation 
 * taking place
 *
 */
public class DistFuture<R> implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = -3891000966480486556L;
	/** Member keeping the internal GLB operation this DistFuture represents */
	@SuppressWarnings("rawtypes")
	GlbOperation operation;
	/** Member keeping the result expected by this DistFuture */
	private R result;
	
	/**
	 * Constructor with a handle to the distributed collection in which the 
	 * result of the operation will be stored
	 * @param r distributed collection handle 
	 */
	DistFuture(R r) {
		result = r;
	}
	
	/**
	 * Places a dependency on the operation represented by this instance. This operation will not start until the operation passed as parameter has completed globally
	 * @param dependency the operation that needs to complete for this operation to start
	 * @return this instance
	 * @throws OperationNotSupportedException is still in development
	 */
	DistFuture<R> after(DistFuture<?> dependency) throws OperationNotSupportedException {
		GlobalLoadBalancer.glb.scheduleOperationAfter(dependency.operation, this.operation);
		return this;
	}
	
	/**
	 * Yields back the result of the operation submitted to the GLB which this
	 * instance represents. 
	 * @return distributed collection handle
	 */
	R result() {
		if (!operation.finished()) {
			waitGlobalTermination();
		}
		return result;
	}
	
	/**
	 * Blocks the progress of the GLB program until the operation represented by this instance has completed on each host.
	 * <p>
	 * Calling this method in a GLB program will cause all previously submitted operations to start. 
	 */
	public void waitGlobalTermination() {
		GlobalLoadBalancer.startAndWait(operation);
	}
}
