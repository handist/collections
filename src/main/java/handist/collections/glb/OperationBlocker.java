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
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ManagedBlocker;
import java.util.concurrent.Semaphore;

import apgas.Constructs;

/**
 * {@link ManagedBlocker} implementation relying on a Semaphore.
 * <p>
 * This class is used to make threads of the {@link ForkJoinPool} used in the
 * APGAS runtime for Java block while guaranteeing that another thread is
 * created in the pool, thus maintaining the desired level of parallelism.
 * <p>
 * This class also ensures that at most a single thread is waiting on this
 * blocker at a time. This is ensured with a secondary semaphore. Any thread
 * that wants to block on this {@link ManagedBlocker} should first call method
 * {@link #allowedToBlock()}. If the method returned {@code true}, then
 *
 * @author Patrick Finnerty
 *
 */
@SuppressWarnings("javadoc")
class OperationBlocker implements ForkJoinPool.ManagedBlocker, Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -124119872365479159L;

    /** Semaphore used for this lock implementation */
    private final Semaphore lock;

    /** Semaphore used to restrict blocking to a single thread at a time */
    private final Semaphore accessThisOperationBlocker;

    /**
     * Constructor Initializes a lock with no permits.
     */
    OperationBlocker() {
        lock = new Semaphore(0);
        accessThisOperationBlocker = new Semaphore(1);
    }

    /**
     * Prospective threads that may want to block on this managed blocker should
     * first call this method. If this method returned {@code true}, then they may
     * block on this operation blocker. Otherwise, a thread is already blocking on
     * this managed blocker and a second thread should not be allowed to block using
     * this instance.
     *
     * @return true if the calling thread can block using this instance
     * @see GlbComputer#operationActivity(GlbOperation)
     */
    /*
     * This method is made synchronized to guarantee correct synchronization with
     * method #unblock(). Before the thread blocking on this instance is released, a
     * token needs to be released in the #accessThisOperationBlocker semaphore.
     */
    synchronized boolean allowedToBlock() {
        return accessThisOperationBlocker.tryAcquire();
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.ForkJoinPool.ManagedBlocker#block()
     */
    @Override
    public boolean block() {
        lock.acquireUninterruptibly();
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.concurrent.ForkJoinPool.ManagedBlocker#isReleasable()
     */
    @Override
    public boolean isReleasable() {
        final boolean gotPermit = lock.tryAcquire();
        return gotPermit;
    }

//    /**
//     * Restores this instance in the state it was when the constructor was called.
//     */
//    void reset() {
//        lock.drainPermits();
//        accessThisOperationBlocker.drainPermits();
//        accessThisOperationBlocker.release();
//    }

    /**
     * Called to unblock the thread that is blocked using this
     * {@link OperationBlocker}.
     */
    synchronized void unblock() {
        if (GlbComputer.TRACE) {
            System.err.println(Constructs.here() + " unblock");
        }
        // accessThisOperationBlocker.drainPermits();
        accessThisOperationBlocker.release(); // Allows for another thread to block
        lock.release(); // Releases the currently blocking thread
    }
}
