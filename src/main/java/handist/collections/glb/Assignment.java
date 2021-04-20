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

/**
 * Portion of work to assign to a worker.
 * <p>
 * The interface represents a portion of a distributed collection on which an
 * operation needs to be performed. Assignments are assigned to workers or
 * otherwise kept in reserve of work inside their
 *
 * @author Patrick Finnerty
 *
 */
interface Assignment extends Serializable, Comparable<Assignment> {

    /**
     * Chooses an operation available in this assignment to be processed by the
     * worker.
     *
     *
     * @return a GlbOperation with work remaining in this assignment
     */
    @SuppressWarnings("rawtypes")
    GlbOperation chooseOperationToProgress();

    @Override
    default int compareTo(Assignment o) {
        return priority() - o.priority();
    }

    /**
     * Indicates if this assignment can be split into 2 new assignments. An
     * assignment is considered splittable if splitting the assignment will result
     * in some work being available for other workers, and there is at least one
     * operation for this assignment which contains more work than the integer
     * quantity specified as parameter.
     *
     * @param qtt number of elements left to process under which the assignment
     *            cannot be split. In order words, the assignment is splittable (and
     *            this method will return {@code true}) if it contains one operation
     *            with at least this number of elements left to process.
     * @return {@code true} if splitting this assignment will result in more work
     *         being available for other workers, {@code false otherwise}
     */
    boolean isSplittable(int qtt);

    /**
     * Returns the highest priority (i.e. lowest value) held by an operation which
     * has work contained in this assignment
     *
     * @return the priority of this assignment, as an integer
     */
    int priority();

    /**
     * Process a certain amount of an assignment on an operation available for the
     * current assignment. Then returns true if there remains some work for this
     * assignment that was progressed in this call, false otherwise.
     *
     * @param qtt amount of work to process
     * @param ws  instance providing service the operation might need
     * @param op  operation to progress in this assignment (should have been
     *            previously obtained from {@link #chooseOperationToProgress()})
     * @return true if there is some work available for the operation that was
     *         progressed
     */
    @SuppressWarnings("javadoc")
    boolean process(int qtt, WorkerService ws, @SuppressWarnings("rawtypes") GlbOperation op);

    /**
     * Splits this assignment and places the created assignments into the GlbTask
     * which is handling the assignments of the distributed collection on which the
     * assignments operate.
     */
    void splitIntoGlbTask();
}
