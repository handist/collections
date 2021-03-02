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
import java.util.ArrayList;
import java.util.HashMap;

import handist.collections.glb.GlbComputer.LifelineToken;

/**
 * Interface of the object which manages the assignments of a distributed
 * collection on a local host.
 *
 * @author Patrick Finnerty
 *
 */
public interface GlbTask extends Serializable {

    /**
     * Asks the GlbTask to answer a lifeline thief. The asynchronous call which
     * effectively answers the remote thief is made inside this method. After which
     * the counters for the local completion of the task are updated. This is
     * necessary since we cannot make an answer with an asynchronous call registered
     * under Finish instances which have completed locally without compromising the
     * termination mechanism.
     *
     * @param token token containing the information about the thief
     * @return true if an answer to the remote thief was made, false if it was
     *         impossible to make such an answer at the time this method was called
     */
    boolean answerLifeline(LifelineToken token);

    /**
     * Assigns some work, i.e. a portion of the underlying collection which needs to
     * undergo an operation to the asking worker
     *
     * @return an assignment which the worker will process, or null if impossible to
     *         assign some work at the time this method was called
     */
    Assignment assignWorkToWorker();

    /**
     * Instructs the GlbTask to merge the assignments given as parameter.
     *
     * @param numbers the map indicating how many assignments contain work for each
     *                operation
     * @param stolen  the list of assign@SuppressWarnings("rawtypes") ments that
     *                were stolen
     */
    void mergeAssignments(@SuppressWarnings("rawtypes") HashMap<GlbOperation, Integer> numbers,
            ArrayList<Assignment> stolen);

    /**
     * Signals this GlbTask that a new operation is available for computation on the
     * underlying distributed collection.
     *
     * @param op the new operation available to workers
     * @return true if initializing the operation resulted in any work being made
     *         available to workers, false otherwise
     */
    boolean newOperation(@SuppressWarnings("rawtypes") GlbOperation op);
}
