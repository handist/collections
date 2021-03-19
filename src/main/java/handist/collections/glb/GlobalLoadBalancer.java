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
     * Singleton
     */
    static GlobalLoadBalancer glb = null;

    /**
     * Helper method used to launch the computations submitted to the global load
     * balancer.
     */
    static void start() {
        while (!glb.operationsStaged.isEmpty()) {
            final GlbOperation<?, ?, ?, ?, ?> op = glb.operationsStaged.poll();
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
    static void startAndWait(GlbOperation<?, ?, ?, ?, ?> operation) {
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
     * @param program program to launch under GLB
     * @return collection of all the Exceptions that occurred during the glb program
     */
    public synchronized static ArrayList<Exception> underGLB(SerializableJob program) {
        if (GlobalLoadBalancer.glb == null) {
            // Create a new GlobalLoadBalancer instance that will handle the program
            glb = new GlobalLoadBalancer();

            // Also initialize the GlbComputer on all hosts before trying to submit anything
            GlbComputer.getComputer();

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
                } finally {
                    glb = null; // Destroy the singleton for a new one to be created next time this method is
                    // called
                }
            });
            return exc;
        } else {
            throw new IllegalStateException("Method was called even though another glb program is already running");
        }
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
    void scheduleOperationAfter(GlbOperation<?, ?, ?, ?, ?> before, GlbOperation<?, ?, ?, ?, ?> then) {
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
