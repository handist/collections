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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ForkJoinPool;

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
@SuppressWarnings("rawtypes")
class GlbOperation<C extends DistributedCollection<T, C>, T, K, D, R>
        implements Serializable, Comparable<GlbOperation> {

    /**
     * Managed Blocker implementation used when waiting for the completion of an
     * operation which has already started.
     *
     * @author Patrick Finnerty
     * @see GlobalLoadBalancer#startAndWait(GlbOperation)
     */
    static class OperationCompletionManagedBlocker implements ForkJoinPool.ManagedBlocker {
        /** Semaphore instance around which this class is implemented */
        private volatile boolean releasable;

        /**
         * Constructor
         *
         * Builds a new managed blocker ready for use
         */
        public OperationCompletionManagedBlocker() {
            releasable = false;
        }

        @Override
        public synchronized boolean block() throws InterruptedException {
            if (!releasable) {
                try {
                    this.wait();
                } catch (final InterruptedException e) {
                    // Ignore the exception
                }
            }
            return releasable;
        }

        @Override
        public boolean isReleasable() {
            return releasable;
        }

        public synchronized void unblock() {
            releasable = true;
            notify();
        }
    }

    /**
     * Enumerator used to describe the state of the current operation.
     *
     */
    enum State {
        /**
         * Value used to describe an operation as "staged", i.e. the operation has been
         * submitted to the GLB but the next blocking operation inside the GLB program
         * has not been reached yet
         */
        STAGED,
        /**
         * Value used to describe this operation as running, i.e. either being processed
         * by workers in the GLB or waiting on some dependencies to complete to start
         * computation
         */
        RUNNING,

        /**
         * Value used to describe an operation as "completed", i.e. all of the
         * assignments have been processed globally.
         */
        TERMINATED
    }

    static int nextPriority = 0;

    /** Serial Version UID */
    private static final long serialVersionUID = -7074061733010237021L;

    /**
     * Adds a completion dependency between the instance provided as parameter and
     * this operation.
     * <p>
     * In case the "before" dependency has already terminated, a dependency/hook
     * pair is not installed as the completion dependency is already satisfied.
     * <p>
     * If the "after" operation was submitted to the GLB before a blocking
     * operation, an {@link IllegalStateException} will be thrown.
     *
     * @param before operation which needs to complete before after can start
     * @param after  operation which will only start when the "before" operation has
     *               completed
     * @throws IllegalStateException if the call attempted to add a dependency on an
     *                               operation which may have already started.
     */
    static void makeDependency(GlbOperation<?, ?, ?, ?, ?> before, GlbOperation<?, ?, ?, ?, ?> after) {
        if (after.state != State.STAGED) {
            throw new IllegalStateException(
                    "Attempted to add a completion dependency on an operation which may have already started");
        }

        synchronized (before) {
            if (before.state == State.TERMINATED) {
                return; // Nothing to install as the dependency is already satisfied
            } else {
                synchronized (after) {
                    after.dependencies.add(before); // protected against concurrent after#dependencySatisfied
                }
                before.addHook(() -> after.dependencySatisfied(before));
            }
        }
    }

    /**
     * Static method used to assign a priority to newly created GlbOperations.
     *
     * @return a unique priority level
     */
    /*
     * Implementation note:
     *
     * Even if there are more than the maximum value contained by integers
     * GlbOperations created over the course of an execution, the fact that this
     * counter loops back into negative values is not an issue.
     *
     * What would be an issue is if there were more that the maximum int value of
     * GlbOperation instances submitted to the GLB at the same time. But that would
     * probably be the result of a programming error rather than a real application.
     */
    private static int priority() {
        return nextPriority++;
    }

    /**
     * Priority of this operation compared to other operations
     */
    int priority;

    /**
     * Variable used to keep track of the state of this operation. It will take the
     * following values in order:
     * <ol>
     * <li>{@link #OPERATION_STAGED}
     * <li>{@link #OPERATION_RUNNING}
     * <li>{@link #OPERATION_TERMINATED}
     * </ol>
     * Any access to this member needs to be done through a synchronized block.
     */
    State state;

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
    private final transient Queue<GlbOperation<?, ?, ?, ?, ?>> dependencies;

    /**
     * List of all the errors that were thrown during this operation's execution.
     * This member will remain null until method {@link #getErrors()} is called.
     */
    transient List<Throwable> errors = null;

    /** Indicates if this operation is terminated */
    // private boolean finished = false;

    /**
     * Handle provided to the programmer inside a glb program to manipulate the
     * result of this operation or setup dependencies.
     */
    DistFuture<R> future;

    /** Jobs to do after completion */
    private transient final List<SerializableJob> hooks;

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

    /**
     * Class that should be used as the lifeline for a collection
     */
    final Class lifelineClass;

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
     * @param lifeline             the lifeline class to use with this collection.
     *                             If null, the default lifeline implementation will
     *                             be used. Note that changing the lifeline used by
     *                             a collection within the same GLB programm will
     *                             not work. The lifeline configuration used the
     *                             first time an operation on a collection is
     *                             submitted to the GLB is kept throughout a GLB
     *                             program
     */
    GlbOperation(C c, SerializableBiConsumer<K, WorkerService> op, DistFuture<R> f,
            SerializableSupplier<GlbTask> glbTaskInit, SerializableConsumer<WorkerService> workerInitialization,
            Class lifeline) {
        this(c, op, f, glbTaskInit, workerInitialization, State.STAGED, new GlobalID(), priority(), lifeline);
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
     * @param s                    state of the GlbOperation (staged, running or
     *                             terminated)
     * @param gid                  global id
     */
    private GlbOperation(C c, SerializableBiConsumer<K, WorkerService> op, DistFuture<R> f,
            SerializableSupplier<GlbTask> glbTaskInit, SerializableConsumer<WorkerService> workerInitialization,
            State s, GlobalID gid, int priorityLevel, Class lifeline) {
        collection = c;
        operation = op;
        future = f; // We need a 2-way link between the GlbOperation and the
        future.operation = this; // DistFuture
        hooks = new ArrayList<>();
        dependencies = new LinkedList<>();
        initializerOfGlbTask = glbTaskInit;
        workerInit = workerInitialization;
        state = s;
        id = gid;
        priority = priorityLevel;
        lifelineClass = lifeline; // may be null
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
     * The comparable interface is used so that the GlbOperations can be compared by
     * their priority. This helps us sort the GlbOperations in the various
     * {@link TreeMap} and {@link ConcurrentSkipListMap} used by the GLB runtime
     * which influence the order in which operations are processed.
     */
    @Override
    public int compareTo(GlbOperation o) {
        return priority - o.priority;
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
        // The state "running" needs to be set before calling this method
        assertEquals(State.RUNNING, state);
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

        synchronized (this) {
            state = State.TERMINATED;
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

        // finished = true;

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
        final boolean removed = dependencies.remove(dep);
        assertTrue(dep + " was not a dependency of " + this + " attempted to unblock " + this + " anyway.", removed);

        if (state == State.RUNNING && dependencies.isEmpty()) {
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
        return state == State.TERMINATED;
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
        assertEquals(State.TERMINATED, state);
        if (errors == null) { // If this method was not previously called
            errors = new ArrayList<>();
            for (final Place p : collection.placeGroup().places()) {
                final ArrayList<Throwable> remoteErrors = at(p, () -> { // Synchronous call. Maybe we can do better?
                    return GlbComputer.getComputer().operationErrors.get(this);
                });
                if (remoteErrors != null) {
                    errors.addAll(remoteErrors);
                }

            }
        }

        return errors;
    }

    /**
     * Indicates if this operation has uncompleted dependencies.
     *
     * @return true if other operation need to complete before this operation can
     *         start, false otherwise
     */
    public boolean hasDependencies() {
        return !dependencies.isEmpty();
    }

    @Override
    public int hashCode() {
        return (int) id.gid();
    }
}
