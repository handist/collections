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

import handist.collections.dist.util.Pair;
import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static apgas.Constructs.async;
import static apgas.Constructs.finish;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("deprecation")
public class TestProduct {
    static class Element {
        final long index;
        AtomicLong sum = new AtomicLong(0);
        Element(long index) { this.index = index; }
    }


    @Test
    public void test0() {
        Chunk<Element> chunk = new Chunk<Element>(new LongRange(10, 20), (Long index) -> new Element(index));
        RangedListProduct<Element, Element> pro = new RangedListProduct<>(chunk, chunk);

        pro.forEach((Pair<Element, Element> pair) -> {
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });

        LongRange range = chunk.getRange();
        long result = (range.from + range.to - 1) * range.size();
        for (Element e : chunk) {
            assertEquals(result, e.sum.get());
            e.sum.set(0);
        }
        pro.forEach((long row, long column, Pair<Element, Element> pair) -> {
            if (row < 11) return;
            if (column < 11) return;
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });
        long result2 = (range.from + range.to - 1 + 1) * (range.size() - 1);
        chunk.forEach((long index, Element e) -> {
            assertEquals(index == 10 ? 0 : result2, e.sum.get());
            e.sum.set(0);
        });

        List<SquareRangedList<Pair<Element, Element>>> split = pro.split(2, 2);
        split.forEach((sub) -> {
            System.out.println("Range" + sub.getRange());
            sub.forEach((Pair<Element, Element> pair) -> {
                pair.second.sum.addAndGet(pair.first.index);
                pair.first.sum.addAndGet(pair.second.index);
            });
        });

        chunk.forEach((long index, Element e) -> {
            assertEquals(result, e.sum.get());
            e.sum.set(0);
        });

        RangedListProduct<Element, Element> proH = new RangedListProduct<>(chunk, chunk, true);
        proH.forEach((Pair<Element, Element> pair) -> {
            pair.second.sum.addAndGet(pair.first.index);
            pair.first.sum.addAndGet(pair.second.index);
        });
        long result3 = (range.from + range.to - 1) * range.size() / 2;
        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
        Collection<SquareRangedList<Pair<Element, Element>>> split3 = proH.split(3, 3);
        for (SquareRangedList<Pair<Element, Element>> pro3 : split3) {
            pro3.forEach((Pair<Element, Element> pair) -> {
                pair.second.sum.addAndGet(pair.first.index);
                pair.first.sum.addAndGet(pair.second.index);
            });
        }
        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
        Collection<SquareRangedList<Pair<Element, Element>>> split4 = proH.split(3, 5);
        for (SquareRangedList<Pair<Element, Element>> pro4 : split4) {
            pro4.forEach((Pair<Element, Element> pair) -> {
                pair.second.sum.addAndGet(pair.first.index);
                pair.first.sum.addAndGet(pair.second.index);
            });
        }
        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
        List<List<RangedListProduct<Element, Element>>> ss = proH.splitN2(3, 7, 4, true);
        finish(() -> {
            ss.forEach((ones) -> {
                ones.forEach((one) -> {
                    one.forEach((pair) -> {
                        pair.second.sum.addAndGet(pair.first.index);
                        pair.first.sum.addAndGet(pair.second.index);
                    });
                });
            });
        });
        chunk.forEach((long index, Element e) -> {
            assertEquals(result3 - index, e.sum.get());
            e.sum.set(0);
        });
    }
}
