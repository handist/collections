package handist.collections.glb;

import java.io.Serializable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.Semaphore;

/**
 * {@link ManagedBlocker} implementation relying on a Semaphore.
 * <p>
 * This class is used to make threads of the {@link ForkJoinPool} used in the
 * APGAS runtime for Java block while guaranteeing that another thread is
 * created in the pool, thus mainting the desired level of parallelism.
 *
 * @author Patrick Finnerty
 *
 */
public class SemaphoreBlocker implements ForkJoinPool.ManagedBlocker, Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -124119872365479159L;

    /** Semaphore used for this lock implementation */
    final Semaphore lock;

    /**
     * Constructor Initializes a lock with no permits.
     */
    public SemaphoreBlocker() {
	lock = new Semaphore(0);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.ForkJoinPool.ManagedBlocker#block()
     */
    @Override
    public boolean block() {
	try {
	    lock.acquire();
	} catch (final InterruptedException e) {
	    e.printStackTrace();
	    block();
	}
	return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.ForkJoinPool.ManagedBlocker#isReleasable()
     */
    @Override
    public boolean isReleasable() {
	return lock.tryAcquire();
    }

    /**
     * Drains all the permits in this lock.
     */
    public void reset() {
	lock.drainPermits();
    }

    /**
     * Called to unblock the thread that is blocked using this
     * {@link SemaphoreBlocker}.
     */
    public void unblock() {
	// Avoids unnecessary accumulation of permits in the Lock. In our situation, a
	// maximum of one permit is sufficient.
	lock.drainPermits();
	lock.release();
    }
}