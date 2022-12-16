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

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import apgas.ExtendedConstructs;
import apgas.Job;
import apgas.Place;
import apgas.SerializableJob;
import apgas.impl.Finish;
import handist.collections.dist.DistLog;
import handist.collections.dist.DistributedCollection;
import handist.collections.glb.GlbOperation.OperationCompletionManagedBlocker;
import handist.collections.glb.GlbOperation.State;

/**
 * Class presenting the static method under which GLB operations can operate.
 * Programmers may choose to statically impoirt this class to avoid the lengthy
 * {@code GlobalLoadBalancer#underGLB(SerializableJob)} call in their program.
 *
 * @author Patrick Finnerty
 *
 */
public class GlobalLoadBalancer {

    /**
     * Key used to gather the events related to the GLB system
     */
    public static final String LOGKEY_GLB = "glb_event";

    /**
     * Key used to gather the events related to the creation, interruption, and
     * termination of asynchronous worker activities
     */
    public static final String LOGKEY_WORKER = "glb_worker";

    /**
     * Message used to record the number of workers initialized on a host
     */
    public static final String LOG_INITIALIZED_WORKERS = "WorkerInitialized";

    /**
     * Message used to record the moment at which the GlbComputer instance is
     * initialized on a host
     */
    public static final String LOG_INITIALIZED_AT_NANOTIME = "InitializedAtTime";

    /**
     * Message used to signal that a worker has stopped
     */
    public static final String LOG_WORKER_STOPPED = "Worker Stopped";

    /**
     * Message used to record that a worker was unable to answer a lifeline
     */
    public static final String LOG_LIFELINE_NOT_ANSWERED = "Lifeline not answered";

    /**
     * Message used to record that a worker answered a lifeline
     */
    public static final String LOG_LIFELINE_ANSWERED = "Lifeline answered";

    /**
     * Message used to record that a worker resumed after yielding
     */
    public static final String LOG_WORKER_RESUMED = "Worker resumed";

    /**
     * Message used to record that a started yielding so that other activities may
     * run on the host
     */
    public static final String LOG_WORKER_YIELDING = "Worker yielding";

    /**
     * Message used to record that a worker started running
     */
    public static final String LOG_WORKER_STARTED = "Worker Started";

    public static final String LOGKEY_UNDER_GLB = "UnderGlb";

    public static final String LOG_PROGRAM_STARTED = "ProgramStarted";
    public static final String LOG_PROGRAM_ENDED = "ProgramEnded";

    /**
     * Singleton
     */
    static GlobalLoadBalancer glb = null;

    /**
     * Member keeping the logger instance of the previously executed GLB program. It
     * can be retrieved after method {@link #underGLB(SerializableJob)} has
     * completed through method #getPreviousLog();
     */
    static DistLog previousLog = null;

    /**
     * Returns the {@link DistLog} containing the information logged during the
     * previous {@link #underGLB(SerializableJob)} method call.
     * <p>
     * The logs are returned in their non-gathered state, meaning the entries are
     * still distributed across all hosts. Call method
     * {@link DistLog#globalGather()} on the returned {@link DistLog} instance to
     * gather all the logs on the caller host.
     */
    public static DistLog getPreviousLog() {
        return previousLog;
    }

    /**
     * Helper method used to launch the computations staged into the global load
     * balancer. Is non-blocking and allows progress within the
     * {@link #underGLB(SerializableJob)} method to continue after this method is
     * called.
     * <p>
     * You would only need to call this method if you have some tasks other than the
     * GLB computations that should be executed concurrently. In such a situation,
     * stage the various GLB operations by calling their
     * <em>collection.GLB.method()</em> and setup the potential
     * {@link GlbFuture#after(GlbFuture)} completion dependencies between them.
     * Then, call this method. In the remainder of the ongoing block, you may lay
     * out the other activities that should run while the GLB computations
     * previously staged are being computed in the background.
     */
    public static void start() {
        glb.startComputation();
    }

    /**
     * Helper method used to launch the operations submitted to the global load
     * balancer. In addition, this method will only return when the specified
     * operation completes
     *
     * @param operation the operation whose global termination is be waited upon
     */
    static void startAndWait(GlbOperation<?, ?, ?, ?, ?, ?> operation) {
        // Install a hook on the operation on which we are going to wait
        OperationCompletionManagedBlocker b = null;
        synchronized (operation) {
            if (operation.state != State.TERMINATED) {
                // Install a hook to "unblock the SempaphoreBlocker
                b = new OperationCompletionManagedBlocker();
                final OperationCompletionManagedBlocker blocker = b; // final field for lambda expression
                operation.addHook(() -> blocker.unblock());
            }
        }

        start(); // Start all other operations that were submitted to the GLB

        if (b != null) { // b is different from null iff the operation has not yet terminated
            try {
                // Block until woken up by the installed hook
                ForkJoinPool.managedBlock(b);
            } catch (final InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Method to call to launch a GLB program. A single call to this method can be
     * made at the time. If a second call is made while a program is under way, this
     * second call will be blocked.
     *
     * @param logger  existing {@link DistLog} into which the logged events of the
     *                GLB will be kept
     * @param program program to launch under GLB
     * @return collection of all the Exceptions that occurred during the glb program
     */
    public static ArrayList<Exception> underGLB(DistLog logger, SerializableJob program) {
        if (GlobalLoadBalancer.glb == null) {
            // Logger instance for the entire GLB
            previousLog = logger;
            logger.put(LOGKEY_UNDER_GLB, LOG_PROGRAM_STARTED, Long.toString(System.nanoTime()));

            // Create a new GlobalLoadBalancer instance that will handle the program
            glb = new GlobalLoadBalancer();

            // Also initialize the GlbComputer on all hosts before trying to submit anything
            GlbComputer.initializeComputer(logger);

            final ArrayList<Exception> exc = new ArrayList<>();
            finish(() -> {
                try {
                    program.run();
                    start(); // This launches any submitted operation that have not been explicitly launched
                    // inside the provided program
                } catch (final Exception e) {
                    System.err.println("ERROR during GLB program execution");
                    e.printStackTrace();
                    exc.add(e);
                }
            });
            logger.put(LOGKEY_UNDER_GLB, LOG_PROGRAM_ENDED, Long.toString(System.nanoTime()));

            // Destroy the singletons for new ones to be created next time this method is
            // called
            glb = null;

            GlbComputer.destroyGlbComputer();
            // Also reset the priority for the future GlbOperations to be created
            GlbOperation.nextPriority = 0;

            return exc;
        } else {
            throw new IllegalStateException("Method was called even though another glb program is already running");
        }
    }

    /**
     * Method to call to launch a GLB program. A single call to this method can be
     * made at the time. If a second call is made while a program is under way, this
     * second call will be blocked.
     *
     * @param program program to launch under GLB
     * @return collection of all the Exceptions that occurred during the glb program
     */
    public static ArrayList<Exception> underGLB(SerializableJob program) {
        return underGLB(new DistLog(), program);
    }

    /**
     * Member used to collect the finish of each individual operations when a batch
     * is launched
     */
    private final BlockingQueue<Finish> finishCollector;

    /**
     * Collection containing the operation that have been submitted to the GLB as
     * part of a program given to {@link #underGLB(SerializableJob)}.
     */
    @SuppressWarnings("rawtypes")
    final HashMap<DistributedCollection, HashSet<GlbOperation<?, ?, ?, ?, ?, ?>>> operationsStaged;

    /**
     * Contains the batches of operations which are "ready" to be started and are
     * waiting for ongoing operations on the same collection to complete before
     * starting.
     */
    @SuppressWarnings("rawtypes")
    final HashMap<DistributedCollection, HashSet<GlbOperation<?, ?, ?, ?, ?, ?>>> operationsReady;

    /**
     * This map keeps track of the operation currently being computed for each
     * distributed collection.
     */
    @SuppressWarnings("rawtypes")
    final HashMap<DistributedCollection, HashSet<GlbOperation<?, ?, ?, ?, ?, ?>>> operationsInComputation;

    /**
     * Private constructor to preserve the singleton pattern
     */
    private GlobalLoadBalancer() {
        operationsStaged = new HashMap<>();
        operationsReady = new HashMap<>();
        operationsInComputation = new HashMap<>();
        finishCollector = new LinkedBlockingQueue<>();
    }

    /**
     * Launches the batch of "ready" computation
     * <p>
     * This helper method should be called from within a synchronized block whose
     * provider is the GlobalLoadBalancer singleton {@link #glb}
     *
     * @param col the collection whose "ready" computation needs to be launched
     */
    @SuppressWarnings("rawtypes")
    private void launchReadyComputationForCollection(DistributedCollection col) throws InterruptedException {
        // First, obtain the group of operations to launch
        final HashSet<GlbOperation<?, ?, ?, ?, ?, ?>> opsToLaunch = operationsReady.get(col);
        final HashSet<GlbOperation<?, ?, ?, ?, ?, ?>> opsRunning = operationsInComputation.get(col);

        assertTrue(opsRunning.isEmpty());

        final int nbOps = opsToLaunch.size();
        final Finish[] finishArray = new Finish[nbOps];
        final GlbOperation[] operationArray = new GlbOperation[nbOps];
        int index = 0;

        for (final GlbOperation<?, ?, ?, ?, ?, ?> op : opsToLaunch) {
            op.openingThreadCanTerminate = new Semaphore(0);
            async(() -> op.openFinish(finishCollector));

            // Record the Operation and the Finish at matching indices in the array
            operationArray[index] = op;
            finishArray[index++] = finishCollector.take();
        }

        // Transfer the operations object from the "ready" to "running" collection
        opsRunning.addAll(opsToLaunch);
        opsToLaunch.clear();

        // Structure finish which starts the whole operation batch
        finish(() -> {
            for (final Place p : places()) {
                ExtendedConstructs.asyncAtWithCoFinish(p, () -> {
                    GlbComputer.getComputer().newOperationBatch(operationArray, finishArray); // TODO adapt this method
                }, finishArray);
            }
        });

        // Unblock the thread which opened the finish block for the operation
        for (final GlbOperation op : opsRunning) {
            op.openingThreadCanTerminate.release();
        }
    }

    /**
     * Makes the global load balancer start the operation given as second argument
     * after the operation represented by the first argument had completed globally.
     *
     * @param before operation to terminate before the second argument can start
     * @param then   operation to start after the first argument has completed
     */
    @SuppressWarnings("rawtypes")
    synchronized void scheduleOperationAfter(GlbOperation before, GlbOperation then) {
        GlbOperation.makeDependency(before, then);
    }

    /**
     * This helper method does multiple things:
     * <ol>
     * <li>Change all the "staged" operations into the "ready" state,
     * <li>Discard the operation that have some pending completion dependencies,
     * they will be placed back into the "ready" batch when their dependencies are
     * completed.
     * <li>If there is no ongoing computation on the collection involved
     * </ol>
     */
    private synchronized void startComputation() {
        for (final DistributedCollection<?, ?> collection : operationsStaged.keySet()) {
            final HashSet<GlbOperation<?, ?, ?, ?, ?, ?>> stagedOperations = operationsStaged.get(collection);

            // Turn all operations to state "ready"
            stagedOperations.forEach(op -> op.state = State.READY);

            // Remove the operations which have dependencies
            stagedOperations.removeIf(op -> op.hasDependencies());
            final HashSet<GlbOperation<?, ?, ?, ?, ?, ?>> readyOperations = operationsReady.get(collection);
            readyOperations.addAll(stagedOperations);
            stagedOperations.clear();

            // All "staged" operations have been placed in "ready" stage
            // Now we launch the batch of operation iff there are no running operations for
            // the involved collection
            if (operationsInComputation.get(collection).isEmpty() && !readyOperations.isEmpty()) {
                try {
                    launchReadyComputationForCollection(collection);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Helper method used by the various Glb handles of the distributed collections
     * to submit an operation to the Glb.
     *
     * @param operation operation to perform on a distributed collection
     */
    @SuppressWarnings("rawtypes")
    synchronized void submit(GlbOperation operation) {
        // Prepare the various sets of operations necessary
        final DistributedCollection col = operation.collection;
        final HashSet<GlbOperation<?, ?, ?, ?, ?, ?>> stagedOps = operationsStaged.computeIfAbsent(col,
                k -> new HashSet<>());
        operationsReady.computeIfAbsent(col, k -> new HashSet<>());
        operationsInComputation.computeIfAbsent(col, k -> new HashSet<>());

        stagedOps.add(operation);
    }

    /**
     * Method called by the operations as they complete. The terminating hooks are
     * executed and the operation state is switched to "Terminated"
     *
     * @param op operation which has just completed execution
     */
    @SuppressWarnings("rawtypes")
    synchronized void terminateComputation(GlbOperation<?, ?, ?, ?, ?, ?> op) {
        // Remove the operation from the set of running operations
        final DistributedCollection col = op.collection;
        final boolean opRemoved = operationsInComputation.get(col).remove(op);
        // Sanity check
        assertTrue("terminateComputation was called with operation not registered in running ops", opRemoved);

        // State is updated before performing the hooks
        op.state = State.TERMINATED;

        // The operation has completed, we execute the various hooks it may have
        for (final Job h : op.hooks) {
            try {
                h.run();
            } catch (final Exception e) {
                System.err.println("Exception was thrown as part of operation" + this);
                e.printStackTrace();
            }
        }

        // Check if a batch of "ready" operations can be launched as a result of this
        // operation completing.
        // This could be the case for two reasons:
        // 1. Some other operations on the same collection where staged. If `op` was the
        // last running operation on this collection, the next batch can be launched
        // 2. `op` was dependency for an operation. As this dependency is now cleared,
        // an operation may have been added back to the batch of operations to launch.
        for (final DistributedCollection c : operationsReady.keySet()) {
            if (!operationsReady.get(c).isEmpty() && operationsInComputation.get(c).isEmpty()) {
                try {
                    launchReadyComputationForCollection(c);
                } catch (final InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
//        if (runningOperations.isEmpty() && !operationsReady.get(col).isEmpty()) {
//            // If there are no more operations running for that collection and some
//            // operations for that collection are "ready", launch that batch of "ready"
//            // computation
//            try {
//                launchReadyComputationForCollection(col);
//            } catch (final InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
    }
}
