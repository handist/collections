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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.ForkJoinPool;

import apgas.SerializableJob;
import handist.collections.dist.DistLog;
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
     * Message used to record the number of workers initalized on a host
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
     * Message used to record that a started yielding so that other activites may
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
     * balancer. Is not blocking.
     * <p>
     * You would only need to call this method if you have some tasks other than the
     * GLB computations that should be executed concurrently. In such a situation,
     * stage the various GLB operations by calling their
     * <em>collection.GLB.method()</em> and setup the potential
     * {@link DistFuture#after(DistFuture)} completion dependencies. Then, call this
     * method. In the remainder of the ongoing block, you lay out the other
     * activities that should run while the GLB computations previously staged are
     * being computed.
     */
    public static void start() {
        while (!glb.operationsStaged.isEmpty()) {
            final GlbOperation<?, ?, ?, ?, ?, ?> op = glb.operationsStaged.poll();
            boolean needsToBeLaunched;
            synchronized (op) {
                op.state = GlbOperation.State.RUNNING;
                needsToBeLaunched = !op.hasDependencies();
            }

            if (needsToBeLaunched) {
                async(() -> op.compute());
            }
        }
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
     * Collection containing the operation that have been submitted to the GLB as
     * part of a program given to {@link #underGLB(SerializableJob)}.
     */
    @SuppressWarnings("rawtypes")
    private final LinkedList<GlbOperation> operationsStaged;

    /**
     * Private constructor to preserve the singleton pattern
     */
    private GlobalLoadBalancer() {
        operationsStaged = new LinkedList<>();
    }

    /**
     * Makes the global load balancer start the operation given as second argument
     * after the operation represented by the first argument had completed globally.
     *
     * @param before operation to terminate before the second argument can start
     * @param then   operation to start after the first argument has completed
     */
    void scheduleOperationAfter(GlbOperation<?, ?, ?, ?, ?, ?> before, GlbOperation<?, ?, ?, ?, ?, ?> then) {
        GlbOperation.makeDependency(before, then);
    }

    /**
     * Helper method used by the various Glb handles of the distributed collections
     * to submit an operation to the Glb.
     *
     * @param operation operation to perform on a distributed collection
     */
    void submit(@SuppressWarnings("rawtypes") GlbOperation operation) {
        operationsStaged.add(operation);
    }
}
