/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.function;

import java.io.Serializable;

import apgas.Place;
import handist.collections.dist.util.ObjectInput;

public interface DeSerializerUsingPlace extends Serializable{
	void accept(ObjectInput in, Place p) throws Exception;
}
