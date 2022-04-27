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

/**
 * Interface presenting the services a worker can provide to GlbOperations. As
 * of now, a single service consisting in keeping a dedicated object for a
 * certain operation is implemented.
 *
 * @author Patrick Finnerty
 *
 */
interface WorkerService {

    /**
     * Binds an object to this worker for later use by the specified operation.
     *
     * @param key the object used as key to store an object in each wokrer
     * @param o   object to bind to the worker for later use
     */
    void attachOperationObject(Object key, Object o);

    /**
     * Retrieves the object bound to the specified operation kept by this worker. If
     * no object was previously bound to the worker for the specified operation,
     * this method will return null.
     *
     * @param key the object used as key to store the object used for the
     *            computation
     * @return the worker-bound object kept for the specified operation
     */
    Object retrieveOperationObject(Object key);

    /**
     * Method called to store any throwable that was thrown from the
     * lambda-expression provided by the user as parameter to a GLB operation. This
     * method is in charge of keeping the Throwable until the user calls
     * {@link GlbFuture#getErrors()} which will return any Throwable that was
     * thrown by the user-specified lambda-expression during the GLB operation.
     *
     * @param t The Throwable that was thrown
     */
    void throwableInOperation(Throwable t);
}
