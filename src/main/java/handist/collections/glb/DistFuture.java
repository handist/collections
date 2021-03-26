/*******************************************************************************
 * Copyright (c) 2021 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under
 * the terms of the Eclipse Public License 1.0 which accompanies this
 * distribution,
 * and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 ******************************************************************************/
package handist.collections.glb;

import java.io.Serializable;
import java.util.List;

import javax.naming.OperationNotSupportedException;

/**
 * DistFuture represents the progress of an operation that can take place under
 * GLB
 *
 * @author Patrick Finnerty
 * @param <R> type of the distributed collection returned by the operation
 *            taking place
 *
 */
public class DistFuture<R> implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -3891000966480486556L;
    /** Member keeping the internal GLB operation this DistFuture represents */
    @SuppressWarnings("rawtypes")
    GlbOperation operation;
    /** Member keeping the result expected by this DistFuture */
    private final R result;

    /**
     * Constructor with a handle to the distributed collection in which the result
     * of the operation will be stored
     *
     * @param r distributed collection handle
     */
    DistFuture(R r) {
        result = r;
    }

    /**
     * Places a dependency on the operation represented by this instance. This
     * operation will not start until the operation passed as parameter has
     * completed globally
     *
     * @param dependency the operation that needs to complete for this operation to
     *                   start
     * @return this instance
     * @throws OperationNotSupportedException is still in development
     */
    public DistFuture<R> after(DistFuture<?> dependency) throws OperationNotSupportedException {
        GlobalLoadBalancer.glb.scheduleOperationAfter(dependency.operation, this.operation);
        return this;
    }

    /**
     * Returns the exceptions thrown by the user-provided lambda expression during
     * the computation. If the computation was not previously started, or if it is
     * ongoing, this method will block until the operation terminates and return the
     * exceptions that were thrown during the operation.
     *
     * @return All {@link Throwable}s thrown during the operation
     */
    @SuppressWarnings("unchecked")
    public List<Throwable> getErrors() {
        if (!operation.finished()) {
            waitGlobalTermination();
        }
        return operation.getErrors();
    }

    /**
     * Yields back the result of the operation submitted to the GLB which this
     * instance represents.
     *
     * @return distributed collection handle
     */
    public R result() {
        if (!operation.finished()) {
            waitGlobalTermination();
        }
        return result;
    }

    /**
     * Blocks the progress of the GLB program until the operation represented by
     * this instance has completed on each host.
     * <p>
     * Calling this method in a GLB program will cause all previously submitted
     * operations to start.
     */
    public void waitGlobalTermination() {
        GlobalLoadBalancer.startAndWait(operation);
    }
}
