package handist.collections.accumulator;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import com.hazelcast.util.function.BiConsumer;

import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;

public class TestThreadLocalAccumulator {

    /** Boxing Element class for Accumulator */
    public class Box {
        public long n2;

        public Box(long i) {
            this.n2 = i;
        }
    }

    /** Element Class for Chunk */
    public class Element {
        public long n1, n2;

        public Element(long i) {
            n1 = i;
            n2 = 0;
        }
    }

    private final int nThreads = 4;

    private ChunkedList<Element> chunkedList;
    private Chunk<Element> chunk0To5;
    private Chunk<Element> chunk5To15;
    private Chunk<Element> chunk15To30;

    /** each thread's range of responsibility, each range is calculated twice */
    private final LongRange[] rangeForThread = { new LongRange(0, 10), new LongRange(0, 20), new LongRange(10, 30),
            new LongRange(20, 30) };

    private ThreadLocalAccumulator<Element, Box> accAllRange;
//    private ThreadLocalAccumulator<Element, Box> accRequiredRange;
//    private ThreadLocalAccumulator<Element, Box> accBlockRange;

    @Before
    public void setup() {
        chunkedList = new ChunkedList<>();
        chunk0To5 = new Chunk<>(new LongRange(0, 5), (index) -> new Element(index));
        chunk5To15 = new Chunk<>(new LongRange(5, 15), (index) -> new Element(index));
        chunk15To30 = new Chunk<>(new LongRange(15, 30), (index) -> new Element(index));
        chunkedList.add(chunk0To5);
        chunkedList.add(chunk5To15);
        chunkedList.add(chunk15To30);

        final Function<Long, Box> initFunc = (index) -> {
            return new Box(0);
        };
        final BiConsumer<Element, Box> reduceFunc = (elem, box) -> {
            elem.n2 += box.n2;
        };
        accAllRange = new AccumulatorCompleteRange<>(chunkedList, initFunc, reduceFunc);
        // TODO commented out for now, restore when we implement these variants as well.
//        accRequiredRange = new AccumulatorRequiredRange<>(chunkedList, initFunc, reduceFunc);
//        accBlockRange = new AccumulatorBlockRange<>(chunkedList, 10, initFunc, reduceFunc);
    }

//    @Test
//    public void testBlockRangeAquire() {
//        finish(() -> {
//            for (int i = 0; i < nThreads; i++) {
//                final int i_th = i;
//                async(() -> {
//                    final LongRange range1 = rangeForThread[i_th];
//                    final LongRange range2 = rangeForThread[(i_th + 1) % 4];
//
//                    ChunkedList<Box> a = accBlockRange.acquire(range1);
//                    assertEquals(range1.size(), a.size());
//                    for (final LongRange r : accBlockRange.ranges()) {
//                        assertTrue(range1.isOverlapped(r));
//                    }
//
//                    a = accBlockRange.acquire(rangeForThread[(i_th + 1) % 4]);
//                    assertEquals(range2.size(), a.size());
//                    for (final LongRange r : accBlockRange.ranges()) {
//                        assertTrue((range1.isOverlapped(r) || range2.isOverlapped(r)));
//                    }
//                });
//            }
//        });
//    }
//
//    @Test
//    public void testBlockRangeForEachWithTarget() {
//        finish(() -> {
//            for (int i = 0; i < nThreads; i++) {
//                final int i_th = i;
//                async(() -> {
//                    accBlockRange.forEachWithTarget(rangeForThread[i_th], (element, box) -> {
//                        box.n2 += element.n1;
//                    });
//                });
//            }
//        });
//        // accBlockRange.debugPrint();
//    }
//
//    @Test
//    public void testBlockRangeMerge() {
//        testBlockRangeForEachWithTarget();
//        accBlockRange.merge();
//        // check result
//        chunkedList.forEach((element) -> {
//            assertEquals(element.n1 * 2, element.n2);
//        });
//    }
//
//    @Test
//    public void testBlockRangeParallelMerge() {
//        testBlockRangeForEachWithTarget();
//        accBlockRange.parallelMerge(4);
//        // check result
//        chunkedList.forEach((element) -> {
//            assertEquals(element.n1 * 2, element.n2);
//        });
//    }

    @Test
    public void testCompleteRangeAcquire() {
        finish(() -> {
            for (int i = 0; i < nThreads; i++) {
                final int i_th = i;
                async(() -> {
                    final ChunkedList<Box> a = accAllRange.acquire(rangeForThread[i_th]);
                    assertEquals(chunkedList.ranges(), a.ranges());
                });
            }
        });
    }

    @Test
    public void testCompleteRangeForEachWithTarget() {
        finish(() -> {
            for (int i = 0; i < nThreads; i++) {
                final int i_th = i;
                async(() -> {
                    accAllRange.forEachWithTarget(rangeForThread[i_th], (element, box) -> {
                        box.n2 += element.n1;
                    });
                    assertEquals(chunkedList.ranges(), accAllRange.ranges());
                });
            }
        });
        // accAllRange.debugPrint();
    }

    @Test
    public void testCompleteRangeMerge() {
        testCompleteRangeForEachWithTarget();
        accAllRange.merge();
        // check result
        chunkedList.forEach((element) -> {
            assertEquals(element.n1 * 2, element.n2);
        });
    }

    @Test
    public void testCompleteRangeParallelMerge() {
        testCompleteRangeForEachWithTarget();
        accAllRange.parallelMerge(4);
        // check result
        chunkedList.forEach((element) -> {
            assertEquals(element.n1 * 2, element.n2);
        });
    }

//    @Test
//    public void testRequiredRangeForEachWithTarget() {
//        finish(() -> {
//            for (int i = 0; i < nThreads; i++) {
//                final int i_th = i;
//                async(() -> {
//                    accRequiredRange.forEachWithTarget(rangeForThread[i_th], (element, box) -> {
//                        box.n2 += element.n1;
//                    });
//                });
//            }
//        });
//        // accRequiredRange.debugPrint();
//    }
//
//    @Test
//    public void testRequiredRangeMerge() {
//        testRequiredRangeForEachWithTarget();
//        accRequiredRange.merge();
//        // check result
//        chunkedList.forEach((element) -> {
//            assertEquals(element.n1 * 2, element.n2);
//        });
//    }
//
//    @Test
//    public void testRequiredRangeParallelMerge() {
//        testRequiredRangeForEachWithTarget();
//        accRequiredRange.parallelMerge(4);
//        // check result
//        chunkedList.forEach((element) -> {
//            assertEquals(element.n1 * 2, element.n2);
//        });
//    }

}
