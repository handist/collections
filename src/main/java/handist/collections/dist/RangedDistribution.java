/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
/* 要素を列挙できるタイプの Distribution, IterableDistribution などに改名すべ?　*/
/* 取り敢えずは、Long 専用で。本来 Range[K] { K <: Comparable} 対応にすべきかな？ */
package handist.collections.dist;

import java.util.Map;

import apgas.Place;

interface RangedDistribution<R> {
    public Map<R, Place> placeRanges(R range);
}
