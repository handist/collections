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
package handist.collections.dist;

import java.io.Serializable;

/**
 * Class used as element in the tests of the distributed collections
 */
class Element implements Serializable {
    /** Serial Version UID */
    private static final long serialVersionUID = -2659467143487621997L;

    /** String contained in the element */
    String s;

    /**
     * Constructor with initial String value
     *
     * @param string initial value of the contained string, may be null
     */
    Element(String string) {
        s = string;
    }

    /**
     * Returns the contained String with "El: " prefix
     */
    @Override
    public String toString() {
        return "El: " + s;
    }
}
