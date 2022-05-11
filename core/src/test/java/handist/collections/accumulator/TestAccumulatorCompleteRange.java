package handist.collections.accumulator;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Test;

import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;
import handist.collections.accumulator.Accumulator.ThreadLocalAccumulator;

public class TestAccumulatorCompleteRange {

    /**
     * Modifiable integer object
     */
    public static class IntValue {
        int value;

        public IntValue(int i) {
            value = i;
        }
    }

    /**
     * Ranges prepared for the ChunkedList used in all the tests of this class. We
     * use contiguous ranges to simply loops.
     */
    final static LongRange lr1 = new LongRange(0, 100);
    final static LongRange lr2 = new LongRange(100, 200);
    final static LongRange lr3 = new LongRange(200, 400);

    /**
     * Initializer to prepare the contents of the chunks contained by the
     * ChunkedList
     */
    final static Function<Long, IntValue> chunkInitializer = l -> {
        return new IntValue(l.intValue());
    };
    final static int totalSize = (int) (lr1.size() + lr2.size() + lr3.size());

    int randomIntArray[];

    /**
     * ChunkedList used as the ultimate target of the "accumulator" computation
     */
    ChunkedList<IntValue> source;

    @Before
    public void before() {
        // Populate the source ChunkedList
        source = new ChunkedList<>();
        source.add(new Chunk<>(lr1, chunkInitializer));
        source.add(new Chunk<>(lr2, chunkInitializer));
        source.add(new Chunk<>(lr3, chunkInitializer));

        // Populate the array with random integer values
        final Random r = new Random(42l);
        randomIntArray = new int[totalSize];
        Arrays.setAll(randomIntArray, i -> {
            return r.nextInt(1000);
        });

        // Populate the ChunkedList
        int arrayIdx = 0;
        for (final long l : lr1) {
            source.get(l).value = randomIntArray[arrayIdx];
            arrayIdx++;
        }
        for (final long l : lr2) {
            source.get(l).value = randomIntArray[arrayIdx];
            arrayIdx++;
        }
        for (final long l : lr3) {
            source.get(l).value = randomIntArray[arrayIdx];
            arrayIdx++;
        }
    }

    @Test
    public void testAllocationOfTLA() {
        final Accumulator<IntValue> accumulator = new AccumulatorCompleteRange<>(source, l -> new IntValue(0));

        // A newly created accumulator has no threadLocalAccumulator yet
        assertTrue(accumulator.threadLocalAccumulators.isEmpty());

        // Check the number of allocated TLAs when asking for some for the first time
        final List<ThreadLocalAccumulator<IntValue>> tlas = accumulator.obtainThreadLocalAccumulators(4);
        assertEquals(4, tlas.size());
        assertEquals(4, accumulator.threadLocalAccumulators.size());

        // Check that no new TLAs are created when asking for fewer than was already
        // initialized
        final List<ThreadLocalAccumulator<IntValue>> tlas2 = accumulator.obtainThreadLocalAccumulators(2);
        assertEquals(2, tlas2.size());
        assertEquals(4, accumulator.threadLocalAccumulators.size());

        // Check that the exact number of extra TLAs are created when asking for more
        // than previously initialized
        final List<ThreadLocalAccumulator<IntValue>> tlas3 = accumulator.obtainThreadLocalAccumulators(6);
        assertEquals(6, tlas3.size());
        assertEquals(6, accumulator.threadLocalAccumulators.size());

        // Check that Reset discards all TLA
        accumulator.reset();
        assertTrue(accumulator.threadLocalAccumulators.isEmpty());
    }

    @Test
    public void testParallelPrefixSum() {
        // Prepare an accumulator
        final Accumulator<IntValue> acc = new AccumulatorCompleteRange<>(source, l -> new IntValue(0));

        // Accumulate values
        source.parallelForEach(acc, (l, sourceInteger, tla) -> {
            for (long prefix = l + 1; prefix < lr3.to; prefix++) {
                // increment by the value of the integer the TLA of all indices to the right
                tla.acquire(prefix).value += sourceInteger.value;
            }
        });

        // Apply update on the "source" ChunkedList
        source.parallelAccept(acc, (IntValue i, IntValue a) -> {
            i.value += a.value;
        });

        // Compute the prefix sum on the randomIntArray and check we obtain the same
        // results
        Arrays.parallelPrefix(randomIntArray, (a, b) -> a + b);

        for (int idx = 0; idx < randomIntArray.length; idx++) {
            assertEquals("at index " + idx, randomIntArray[idx], source.get(idx).value);
        }
    }

    @Test
    public void testPrefixSum() {
        // Prepare an accumulator
        final Accumulator<IntValue> acc = new AccumulatorCompleteRange<>(source, l -> new IntValue(0));

        // Accumulate values
        source.forEach(acc, (sourceIndex, sourceInteger, tla) -> {
            for (long prefix = sourceIndex + 1; prefix < lr3.to; prefix++) {
                // increment by the value of the integer the TLA of all indices to the right
                tla.acquire(prefix).value += sourceInteger.value;
            }
        });

        // Apply update on the "source" ChunkedList
        source.accept(acc, (IntValue i, IntValue a) -> {
            i.value += a.value;
        });

        // Compute the prefix sum on the randomIntArray and check we obtain the same
        // results
        Arrays.parallelPrefix(randomIntArray, (a, b) -> a + b);

        for (int idx = 0; idx < randomIntArray.length; idx++) {
            assertEquals("at index " + idx, randomIntArray[idx], source.get(idx).value);
        }
    }
}
