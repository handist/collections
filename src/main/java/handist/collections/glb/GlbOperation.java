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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import apgas.MultipleException;
import apgas.Place;
import apgas.SerializableJob;
import apgas.util.GlobalID;
import handist.collections.dist.DistributedCollection;
import handist.collections.function.SerializableBiConsumer;
import handist.collections.function.SerializableConsumer;
import handist.collections.function.SerializableSupplier;

/**
 * Operation to perform on a distributed collection under the GLB. This class
 * takes four generic type parameters:
 * <ul>
 * <li>the type of the distributed collection at hand C
 * <li>the type of the elements contained in the collection T
 * <li>the type used to designate elements of the collection to relocate
 * <li>the type of the ditributed collection representing the result of the
 * operation
 *
 * @author Patrick Finnerty
 *
 * @param <C> type of the distributed collection
 * @param <T> type of the individual elements contained by the collection
 * @param <K> type used to identify individual elements in the collection
 * @param <D> type used to identify elements for relocation, may be identical to
 *            K
 * @param <R> type of the distributed collection representing the result of the
 *            operation
 */
class GlbOperation<C extends DistributedCollection<T, C>, T, K, D, R> implements Serializable {
    /** Serial Version UID */
    private static final long serialVersionUID = -7074061733010237021L;

    /**
     * Adds a completion dependency between the instance provided as parameter and
     * this operation.
     *
     * @param before operation which needs to complete before after can start
     * @param after  operation which will only start when the "before" operation has
     *               completed
     */
    static void makeDependency(GlbOperation<?, ?, ?, ?, ?> before, GlbOperation<?, ?, ?, ?, ?> after) {
        after.dependencies.add(before);
        before.addHook(() -> after.dependencySatisfied(before));
    }

    /** Global id for this GlbOperation */
    GlobalID id;

    /** Distributed collection on which this operation is operating */
    C collection;

    /**
     * List of GlbOperations that need to terminate before this one can start.
     * Placing dependencies in this member can be done without protection. However,
     * one a Glb computation has started, no new dependency should be added to this
     * member.
     * <p>
     * When an operation on which this instance depends completes, it removes itself
     * from this member as part of one of its hooks (see member {@link #hooks}). If
     * this member is made empty as a result, that hook will start this instance's
     * computation.
     */
    private final Queue<GlbOperation<?, ?, ?, ?, ?>> dependencies;

    /**
     * List of all the errors that were thrown during this operation's execution.
     * This member will remain null until method {@link #getErrors()} is called.
     */
    transient List<Throwable> errors = null;

    /** Indicates if this operation is terminated */
    private boolean finished = false;

    /**
     * Handle provided to the programmer inside a glb program to manipulate the
     * result of this operation or setup dependencies.
     */
    DistFuture<R> future;

    /** Jobs to do after completion */
    private final List<SerializableJob> hooks;

    /**
     * Initializer which will be called on every host if the GlbTask for the
     * operation was not previously initialized by another GlbOperation.
     */
    SerializableSupplier<GlbTask> initializerOfGlbTask;

    /**
     * The method to be called by workers. It expects an instance of the identifier
     * type K to perform the operation. The second argument (WorkerService) is here
     * to provide special services to the operation in case it requires them.
     */
    SerializableBiConsumer<K, WorkerService> operation;

    /**
     * Method to be called on every worker before this operation can start on a
     * host. May be null, in which case no particular action is needed.
     */
    SerializableConsumer<WorkerService> workerInit;

//    /**
//     * Private constructor which may only be used thorugh reflection by serializers
//     */
//    @SuppressWarnings("unused")
//    private GlbOperation() {
//        hooks = new ArrayList<>();
//        dependencies = new LinkedList<>();
//    }

    /**
     * Constructor for GLB operation. The distributed collection under consideration
     * and the method to be called on it needs to be specified.
     *
     * @param c                    distributed collection on which this operation
     *                             will be applied
     * @param op                   method to call on each local host to perform the
     *                             computation
     * @param f                    object that is presented to the programmer inside
     *                             the GLB program in which the result will be
     *                             stored.
     * @param glbTaskInit          initializer of the class which will handle the
     *                             progression of this operation. It will be used if
     *                             not previously initialized for this collection
     *                             through another collection.
     * @param workerInitialization initialization to be performed on every worker in
     *                             the system before this operation starts. May be
     *                             null is not needed.
     */
    GlbOperation(C c, SerializableBiConsumer<K, WorkerService> op, DistFuture<R> f,
            SerializableSupplier<GlbTask> glbTaskInit, SerializableConsumer<WorkerService> workerInitialization) {
        this(c, op, f, glbTaskInit, workerInitialization, new GlobalID());
    }

    /**
     * Private constructor used when the GlobalID is known.
     *
     * @param c                    the collection on which this operation operates
     * @param op                   the closure which actually performs the work
     * @param f                    the future which will handle the result of this
     *                             operation
     * @param glbTaskInit          initialization that will prepare the manager of
     *                             the assignments of the distributed collection
     * @param workerInitialization initialization that needs to be performed on
     *                             every worker prior to the
     * @param gid                  global id
     */
    private GlbOperation(C c, SerializableBiConsumer<K, WorkerService> op, DistFuture<R> f,
            SerializableSupplier<GlbTask> glbTaskInit, SerializableConsumer<WorkerService> workerInitialization,
            GlobalID gid) {
        collection = c;
        operation = op;
        future = f; // We need a 2-way link between the GlbOperation and the
        future.operation = this; // DistFuture
        hooks = new ArrayList<>();
        dependencies = new LinkedList<>();
        initializerOfGlbTask = glbTaskInit;
        workerInit = workerInitialization;
        id = gid;
        id.putHere(this);
    }

    /**
     * Adds a hook that will be performed upon global termination of this operation.
     *
     * @param j the job to do after this operation has completed
     */
    void addHook(SerializableJob j) {
        hooks.add(j);
    }

    /**
     * Starts this computation and executes the various hooks once it had completed.
     * If an exception occurs during the computation, it will be caught by this
     * method and thrown after all the hooks for this computation are given a chance
     * to be executed. If a hook throws an exception, it will be printed to
     * {@link System#err} but not thrown.
     *
     * @throws MultipleException if an exception was thrown as part of the
     *                           computation
     */
    void compute() {
        MultipleException me = null;
        try {
            collection.placeGroup().broadcastFlat(() -> {
                // The GLB routine for this operation is called from here
                final GlbComputer glb = GlbComputer.getComputer();
                glb.newOperation(this);
            });
        } catch (final MultipleException e) {
            me = e;
        }
        // The operation has completed, we execute the various hooks it may have
        for (final SerializableJob h : hooks) {
            try {
                h.run();
            } catch (final Exception e) {
                System.err.println("Exception was thrown as part of operation" + this);
                e.printStackTrace();
            }
        }

        finished = true;
        // If a MultipleException was caught, throw it
        if (me != null) {
            throw me;
        }
    }

    /**
     * Method called by a GlbOperation when it has completed and needs to notify
     * this instance which is waiting for that dependency to complete. If all the
     * dependencies of this instance are satisfied as a result, launches the
     * computation of this instance.
     * <p>
     * This method is synchronized to prevent multiple dependencies from
     * concurrently manipulate the {@link #dependencies} collection and launching
     * this computation multiple times.
     *
     * @param dep the operation which has completed
     */
    /*
     * Correct programming of the load balancer ensures that no operation will try
     * to signal that it has completed to an operation of which it is not a
     * dependency, or signal its completion multiple times. However the current
     * implementation elegantly allows such inconsistent cases without any adverse
     * effects. Only when assertions are activated with command line option -ea
     * (enable assertions) that such a case would throw an assertion exception in
     * this method
     */
    private synchronized void dependencySatisfied(GlbOperation<?, ?, ?, ?, ?> dep) {
        assertTrue(dep + " was not a dependency of " + this + " attempted to unblock " + this + " anyway.",
                dependencies.remove(dep));

        if (dependencies.isEmpty()) {
            async(() -> this.compute());
        }
    }

    /**
     * GlbOperation are considered the same if they share the same global id. Other
     * members are not checked. This could a problem if GlobalID instances were
     * re-used carelessly but should otherwise be fine. As GlbOperation's
     * constructor does not allow for an arbitrary id to be given at initialization,
     * this is unlikely to become a problem.
     */
    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object o) {
        if (o instanceof GlbOperation) {
            return id.equals(((GlbOperation) o).id);
        } else {
            return false;
        }
    }

    /**
     * Indicates if this operation has been completed
     *
     * @return true if this operation has completed, false otherwise
     */
    public boolean finished() {
        return finished;
    }

    /**
     * If not previously called, gathers all the Throwables caught on the various
     * hosts and gathers them into a single list which is then returned.
     * <p>
     * This method should only be called AFTER this operation has completed
     * globally, i.e. if calling the {@link #finished()} method returned
     * {@code true}
     *
     * @return a list of all the throwables that were thrown during the operation
     */
    List<Throwable> getErrors() {
        if (errors == null) { // If this method was not previously called
            errors = new ArrayList<>();
            finish(() -> {
                final Place here = here();
                for (final Place p : collection.placeGroup().places()) {
                    asyncAt(p, () -> {
                        // Gather the errors on the remote host
                        final ArrayList<Throwable> remoteErrors = GlbComputer.getComputer().operationErrors.get(this);
                        if (remoteErrors != null) {
                            // Send them to 'here'
                            asyncAt(here, () -> {
                                final List<Throwable> hereCollection = errors;
                                synchronized (hereCollection) {
                                    hereCollection.addAll(remoteErrors);
                                }
                            });
                        }
                    });
                }
            });

        }

        return errors;
    }

    @Override
    public int hashCode() {
        return (int) id.gid();
    }
}
