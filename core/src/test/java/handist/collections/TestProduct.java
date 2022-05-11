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
package handist.collections;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import handist.collections.dist.util.Pair;

public class TestProduct {
    static class Element {
        final long index;
        AtomicLong sum = new AtomicLong(0);

        Element(long index) {
            this.index = index;
        }
    }

    @Test
    public void test0() {
        final Chunk<Element> chunk = new Chunk<>(new LongRange(10, 20), (Long index) -> new Element(index));
        final SimpleRangedProduct<Element, Element> pro = new SimpleRangedProduct<>(chunk, chunk);

        pro.forEach((Pair<Element, Element> pair) -> {
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });

        final LongRange range = chunk.getRange();
        final long result = (range.from + range.to - 1) * range.size();
        for (final Element e : chunk) {
            assertEquals(result, e.sum.get());
            e.sum.set(0);
        }
        pro.forEach((long row, long column, Pair<Element, Element> pair) -> {
            if (row < 11) {
                return;
            }
            if (column < 11) {
                return;
            }
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });
        final long result2 = (range.from + range.to - 1 + 1) * (range.size() - 1);
        chunk.forEach((long index, Element e) -> {
            assertEquals(index == 10 ? 0 : result2, e.sum.get());
            e.sum.set(0);
        });

        final RangedProductList<Element, Element> split = pro.split(2, 2);
        split.forEach((Pair<Element, Element> pair) -> {
//            System.out.println("Range" + sub.getRange());
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });

        chunk.forEach((long index, Element e) -> {
            assertEquals(result, e.sum.get());
            e.sum.set(0);
        });

        final SimpleRangedProduct<Element, Element> proH = new SimpleRangedProduct<>(chunk, chunk, true);
        proH.forEach((Pair<Element, Element> pair) -> {
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });
        final long result3 = (range.from + range.to - 1) * range.size() / 2;
        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
        final RangedProductList<Element, Element> split3 = proH.split(3, 3);
        split3.forEach((Pair<Element, Element> pair) -> {
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });

        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
        final RangedProductList<Element, Element> split4 = proH.split(3, 5);
        split4.forEach((Pair<Element, Element> pair) -> {
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });

        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
        final RangedProductList<Element, Element> ss = proH.split(3, 7);
        finish(() -> {
            ss.parallelForEachProd(4, (prod) -> {
                prod.forEach((pair) -> {
                    pair.second.sum.addAndGet(pair.first.index);
                    pair.first.sum.addAndGet(pair.second.index);
                });
            });
        });
        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
    }
}
