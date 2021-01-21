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
}
