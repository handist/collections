/**
 *
 */
package handist.collections.glb;

import java.io.Serializable;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;

/**
 * {@link ManagedBlocker} implementation that blocks a thread in the
 * {@link ForkJoinPool} used by the APGAS runtime until a certain moment in
 * time. This class allows for a thread of a {@link ForkJoinPool} to be blocked
 * for most of the time and only "wake-up" to perform some short periodic task.
 * <p>
 * The tuning mechanism as well as the whisperer mechanism rely on this class to
 * be called regularly throughout the computation.
 *
 * @author Patrick Finnerty
 *
 */
public class TimeoutBlocker implements Serializable, ManagedBlocker {

    /** Serial Version UID */
    private static final long serialVersionUID = -3062223968154585707L;

    /**
     * Interval after which any thread waiting on this managed blocker will wake-up
     * on its own.
     */
    private static final long WAKEUP_TIMEOUT = 5000000l;

    /**
     * Timestamp of the next time the thread using this blocker is to be released
     */
    private long nextWakeUpTime;

    /**
     * Member used to circumvent the timeout and make the thread available for
     * computation immediately.
     */
    private boolean unblock = false;

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.ForkJoinPool.ManagedBlocker#block()
     */
    @Override
    public boolean block() throws InterruptedException {
        final long now = System.nanoTime();
        final long toElapse = nextWakeUpTime - now;
        if (toElapse < 0 || unblock) {
            return true;
        } else {
            Thread.sleep(toElapse / 1000000, (int) toElapse % 1000000);
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
        }
        return isReleasable();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.ForkJoinPool.ManagedBlocker#isReleasable()
     */
    @Override
    public boolean isReleasable() {
        return (0 > nextWakeUpTime - System.nanoTime()) || unblock;
    }

    /**
     * Re-enables the blocking mechanism after a call to method {@link #unblock()}
     */
    public void reset() {
        unblock = false;
        nextWakeUpTime = System.nanoTime() + WAKEUP_TIMEOUT;
    }

    /**
     * Sets the next wake-up stamp until which the thread that may use this blocker
     * will be blocked. The thread will be relased when the result of method
     * {@link System#nanoTime()}
     *
     * @param stamp timestamp until which {@link System#nanoTime()}
     */
    public void setNextWakeup(long stamp) {
        nextWakeUpTime = stamp;
    }

    /**
     * Unblocks the thread immediately
     */
    public void unblock() {
        unblock = true;
    }

}
