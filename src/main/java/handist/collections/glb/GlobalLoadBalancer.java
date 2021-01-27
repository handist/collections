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

import apgas.SerializableJob;

public class GlobalLoadBalancer {
    static GlobalLoadBalancer glb = null;

    /**
     * Helper method used to launch the computations submitted to the global load
     * balancer.
     */
    static void start() {
        while (!glb.operationsSubmitted.isEmpty()) {
            final GlbOperation<?, ?, ?, ?, ?> op = glb.operationsSubmitted.poll();
            async(() -> op.compute());
        }
    }

    /**
     * Helper method used to launch the computations submitted to the global load
     * balancer. In addition, this method will only return when the specified
     * operation completes
     *
     * @param operation the operation whose global termination is be waited upon
     */
    static void startAndWait(GlbOperation<?, ?, ?, ?, ?> operation) {
        // It is possible that the operation on which we want to wait was already
        // started, in which case removeFirstOccurence will return false
        final boolean waitOperationPresent = glb.operationsSubmitted.removeFirstOccurrence(operation);

        start(); // Start all other operations that were submitted to the GLB

        if (waitOperationPresent) {
            // launch this operation synchronously
            operation.compute();
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
     * Collection containing the operations that are submitted to the GLB as part of
     * a program given to {@link #underGLB(SerializableJob)}. The operation are kept
     * in this collection until they are removed either when they are launched or if
     * they need to wait on the completion of another operation to be launched.
     */
    @SuppressWarnings("rawtypes")
    LinkedList<GlbOperation> operationsSubmitted;

    /**
     * Private constructor to preserve the singleton pattern
     */
    private GlobalLoadBalancer() {
        operationsSubmitted = new LinkedList<>();
    }

    /**
     * Makes the global load balancer start the operation given as second argument
     * after the operation represented by the first argument had completed globally.
     * <p>
     * The operation given as "then" will be removed from the collection of
     * operations to launch and instead be launched by a hook on the "before"
     * operation.
     *
     * @param before operation to terminate before the second argument can start
     * @param then   operation to start after the first argument has completed
     */
    void scheduleOperationAfter(GlbOperation<?, ?, ?, ?, ?> before, GlbOperation<?, ?, ?, ?, ?> then) {
        // "then" may have already been removed if it has multiple dependencies
        operationsSubmitted.remove(then);
        GlbOperation.makeDependency(before, then);
    }

    /**
     * Helper method used by the various Glb handles of the distributed collections
     * to submit an operation to the Glb.
     *
     * @param operation operation to perform on a distributed collection
     */
    void submit(@SuppressWarnings("rawtypes") GlbOperation operation) {
        operationsSubmitted.add(operation);
    }
}
