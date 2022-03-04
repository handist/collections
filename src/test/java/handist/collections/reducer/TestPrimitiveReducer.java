package handist.collections.reducer;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import handist.collections.Bag;
import handist.collections.Chunk;
import handist.collections.ChunkedList;
import handist.collections.LongRange;

public class TestPrimitiveReducer {

    private class Element {
        boolean b;
        double d;
        float f;
        int i;
        long l;
        short s;

        public Element(long value) {
            b = true;
            d = value;
            f = value;
            i = (int) value;
            l = value;
            s = (short) value;
        }
    }

    private ChunkedList<Element> chunkedList;
    private final LongRange range = new LongRange(0, 8);
    private Bag<Element> bag;
    private final int nbBags = 2;

    private final int[] values = { -5, -1, 5, 2, 4, -5, -4, 5 };
    private final int max = 5, min = -5, sum = 1, prod = 20000;

    private final int parallelism = 2;

    @Before
    public void setup() {
        chunkedList = new ChunkedList<>();
        chunkedList.add(new Chunk<>(range, (Long i) -> {
            return new Element(values[i.intValue()]);
        }));
        setupBag();
    }

    private void setupBag() {
        bag = new Bag<>();
        final int size = (int) range.size() / nbBags;
        for (int i = 0; i < nbBags; i++) {
            final ArrayList<Element> list = new ArrayList<>(size);
            for (int j = i * size; j < (i + 1) * size; j++) {
                list.add(new Element(values[j]));
            }
            bag.addBag(list);
        }
    }

    @Test
    public void testParallelReduceBoolAnd() {
        assertTrue(chunkedList.parallelReduce(parallelism, BoolReducer.Op.AND, e -> e.b));
        chunkedList.get(4).b = false;
        assertFalse(chunkedList.parallelReduce(parallelism, BoolReducer.Op.AND, e -> e.b));

        assertTrue(bag.parallelReduce(parallelism, BoolReducer.Op.AND, e -> e.b));
        assertTrue(bag.isEmpty());
        setupBag();
        bag.forEach(e -> e.b = false);
        assertFalse(bag.parallelReduce(parallelism, BoolReducer.Op.AND, e -> e.b));
    }

    @Test
    public void testParallelReduceBoolOr() {
        assertTrue(chunkedList.parallelReduce(parallelism, BoolReducer.Op.OR, e -> e.b));
        chunkedList.get(4).b = false;
        assertTrue(chunkedList.parallelReduce(parallelism, BoolReducer.Op.OR, e -> e.b));
        chunkedList.forEach(e -> e.b = false);
        assertFalse(chunkedList.parallelReduce(parallelism, BoolReducer.Op.OR, e -> e.b));

        assertTrue(bag.parallelReduce(parallelism, BoolReducer.Op.OR, e -> e.b));
        assertTrue(bag.isEmpty());
        setupBag();
        bag.forEach(e -> e.b = false);
        assertFalse(bag.parallelReduce(parallelism, BoolReducer.Op.OR, e -> e.b));
    }

    @Test
    public void testParallelReduceDouble() {
        assertTrue(Math.abs(sum - chunkedList.parallelReduce(parallelism, DoubleReducer.Op.SUM, e -> e.d)) < 0.01d);
        assertTrue(Math.abs(prod - chunkedList.parallelReduce(parallelism, DoubleReducer.Op.PROD, e -> e.d)) < 0.01d);
        assertTrue(Math.abs(max - chunkedList.parallelReduce(parallelism, DoubleReducer.Op.MAX, e -> e.d)) < 0.01d);
        assertTrue(Math.abs(min - chunkedList.parallelReduce(parallelism, DoubleReducer.Op.MIN, e -> e.d)) < 0.01d);

        assertTrue(Math.abs(sum - bag.parallelReduce(parallelism, DoubleReducer.Op.SUM, e -> e.d)) < 0.01d);
        assertTrue(bag.isEmpty());
        setupBag();
        assertTrue(Math.abs(prod - bag.parallelReduce(parallelism, DoubleReducer.Op.PROD, e -> e.d)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(max - bag.parallelReduce(parallelism, DoubleReducer.Op.MAX, e -> e.d)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(min - bag.parallelReduce(parallelism, DoubleReducer.Op.MIN, e -> e.d)) < 0.01d);
    }

    @Test
    public void testParallelReduceFloat() {
        assertTrue(Math.abs(sum - chunkedList.parallelReduce(parallelism, FloatReducer.Op.SUM, e -> e.f)) < 0.01d);
        assertTrue(Math.abs(prod - chunkedList.parallelReduce(parallelism, FloatReducer.Op.PROD, e -> e.f)) < 0.01d);
        assertTrue(Math.abs(max - chunkedList.parallelReduce(parallelism, FloatReducer.Op.MAX, e -> e.f)) < 0.01d);
        assertTrue(Math.abs(min - chunkedList.parallelReduce(parallelism, FloatReducer.Op.MIN, e -> e.f)) < 0.01d);

        assertTrue(Math.abs(sum - bag.parallelReduce(parallelism, FloatReducer.Op.SUM, e -> e.f)) < 0.01d);
        assertTrue(bag.isEmpty());
        setupBag();
        assertTrue(Math.abs(prod - bag.parallelReduce(parallelism, FloatReducer.Op.PROD, e -> e.f)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(max - bag.parallelReduce(parallelism, FloatReducer.Op.MAX, e -> e.f)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(min - bag.parallelReduce(parallelism, FloatReducer.Op.MIN, e -> e.f)) < 0.01d);
    }

    @Test
    public void testParallelReduceInt() {
        assertEquals(sum, chunkedList.parallelReduce(parallelism, IntReducer.Op.SUM, e -> e.i));
        assertEquals(prod, chunkedList.parallelReduce(parallelism, IntReducer.Op.PROD, e -> e.i));
        assertEquals(max, chunkedList.parallelReduce(parallelism, IntReducer.Op.MAX, e -> e.i));
        assertEquals(min, chunkedList.parallelReduce(parallelism, IntReducer.Op.MIN, e -> e.i));

        assertEquals(sum, bag.parallelReduce(parallelism, IntReducer.Op.SUM, e -> e.i));
        assertTrue(bag.isEmpty());
        setupBag();
        assertEquals(prod, bag.parallelReduce(parallelism, IntReducer.Op.PROD, e -> e.i));
        setupBag();
        assertEquals(max, bag.parallelReduce(parallelism, IntReducer.Op.MAX, e -> e.i));
        setupBag();
        assertEquals(min, bag.parallelReduce(parallelism, IntReducer.Op.MIN, e -> e.i));
    }

    @Test
    public void testParallelReduceLong() {
        assertEquals(sum, chunkedList.parallelReduce(parallelism, LongReducer.Op.SUM, e -> e.l));
        assertEquals(prod, chunkedList.parallelReduce(parallelism, LongReducer.Op.PROD, e -> e.l));
        assertEquals(max, chunkedList.parallelReduce(parallelism, LongReducer.Op.MAX, e -> e.l));
        assertEquals(min, chunkedList.parallelReduce(parallelism, LongReducer.Op.MIN, e -> e.l));

        assertEquals(sum, bag.parallelReduce(parallelism, LongReducer.Op.SUM, e -> e.l));
        assertTrue(bag.isEmpty());
        setupBag();
        assertEquals(prod, bag.parallelReduce(parallelism, LongReducer.Op.PROD, e -> e.l));
        setupBag();
        assertEquals(max, bag.parallelReduce(parallelism, LongReducer.Op.MAX, e -> e.l));
        setupBag();
        assertEquals(min, bag.parallelReduce(parallelism, LongReducer.Op.MIN, e -> e.l));
    }

    @Test
    public void testParallelReduceShort() {
        assertEquals(sum, chunkedList.parallelReduce(parallelism, ShortReducer.Op.SUM, e -> e.s));
        assertEquals(prod, chunkedList.parallelReduce(parallelism, ShortReducer.Op.PROD, e -> e.s));
        assertEquals(max, chunkedList.parallelReduce(parallelism, ShortReducer.Op.MAX, e -> e.s));
        assertEquals(min, chunkedList.parallelReduce(parallelism, ShortReducer.Op.MIN, e -> e.s));

        assertEquals(sum, bag.parallelReduce(parallelism, ShortReducer.Op.SUM, e -> e.s));
        assertTrue(bag.isEmpty());
        setupBag();
        assertEquals(prod, bag.parallelReduce(parallelism, ShortReducer.Op.PROD, e -> e.s));
        setupBag();
        assertEquals(max, bag.parallelReduce(parallelism, ShortReducer.Op.MAX, e -> e.s));
        setupBag();
        assertEquals(min, bag.parallelReduce(parallelism, ShortReducer.Op.MIN, e -> e.s));
    }

    @Test
    public void testReduceBoolAnd() {
        assertTrue(chunkedList.reduce(BoolReducer.Op.AND, e -> e.b));
        chunkedList.get(4).b = false;
        assertFalse(chunkedList.reduce(BoolReducer.Op.AND, e -> e.b));

        assertTrue(bag.reduce(BoolReducer.Op.AND, e -> e.b));
        assertTrue(bag.isEmpty());
        setupBag();
        bag.forEach(e -> e.b = false);
        assertFalse(bag.reduce(BoolReducer.Op.AND, e -> e.b));
    }

    @Test
    public void testReduceBoolOr() {
        assertTrue(chunkedList.reduce(BoolReducer.Op.OR, e -> e.b));
        chunkedList.get(4).b = false;
        assertTrue(chunkedList.reduce(BoolReducer.Op.OR, e -> e.b));
        chunkedList.forEach(e -> e.b = false);
        assertFalse(chunkedList.reduce(BoolReducer.Op.OR, e -> e.b));

        assertTrue(bag.reduce(BoolReducer.Op.OR, e -> e.b));
        assertTrue(bag.isEmpty());
        setupBag();
        bag.forEach(e -> e.b = false);
        assertFalse(bag.reduce(BoolReducer.Op.OR, e -> e.b));
    }

    @Test
    public void testReduceDouble() {
        assertTrue(Math.abs(sum - chunkedList.reduce(DoubleReducer.Op.SUM, e -> e.d)) < 0.01d);
        assertTrue(Math.abs(prod - chunkedList.reduce(DoubleReducer.Op.PROD, e -> e.d)) < 0.01d);
        assertTrue(Math.abs(max - chunkedList.reduce(DoubleReducer.Op.MAX, e -> e.d)) < 0.01d);
        assertTrue(Math.abs(min - chunkedList.reduce(DoubleReducer.Op.MIN, e -> e.d)) < 0.01d);

        assertTrue(Math.abs(sum - bag.reduce(DoubleReducer.Op.SUM, e -> e.d)) < 0.01d);
        assertTrue(bag.isEmpty());
        setupBag();
        assertTrue(Math.abs(prod - bag.reduce(DoubleReducer.Op.PROD, e -> e.d)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(max - bag.reduce(DoubleReducer.Op.MAX, e -> e.d)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(min - bag.reduce(DoubleReducer.Op.MIN, e -> e.d)) < 0.01d);
    }

    @Test
    public void testReduceFloat() {
        assertTrue(Math.abs(sum - chunkedList.reduce(FloatReducer.Op.SUM, e -> e.f)) < 0.01d);
        assertTrue(Math.abs(prod - chunkedList.reduce(FloatReducer.Op.PROD, e -> e.f)) < 0.01d);
        assertTrue(Math.abs(max - chunkedList.reduce(FloatReducer.Op.MAX, e -> e.f)) < 0.01d);
        assertTrue(Math.abs(min - chunkedList.reduce(FloatReducer.Op.MIN, e -> e.f)) < 0.01d);

        assertTrue(Math.abs(sum - bag.reduce(FloatReducer.Op.SUM, e -> e.f)) < 0.01d);
        assertTrue(bag.isEmpty());
        setupBag();
        assertTrue(Math.abs(prod - bag.reduce(FloatReducer.Op.PROD, e -> e.f)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(max - bag.reduce(FloatReducer.Op.MAX, e -> e.f)) < 0.01d);
        setupBag();
        assertTrue(Math.abs(min - bag.reduce(FloatReducer.Op.MIN, e -> e.f)) < 0.01d);
    }

    @Test
    public void testReduceInt() {
        assertEquals(sum, chunkedList.reduce(IntReducer.Op.SUM, e -> e.i));
        assertEquals(prod, chunkedList.reduce(IntReducer.Op.PROD, e -> e.i));
        assertEquals(max, chunkedList.reduce(IntReducer.Op.MAX, e -> e.i));
        assertEquals(min, chunkedList.reduce(IntReducer.Op.MIN, e -> e.i));

        assertEquals(sum, bag.reduce(IntReducer.Op.SUM, e -> e.i));
        assertTrue(bag.isEmpty());
        setupBag();
        assertEquals(prod, bag.reduce(IntReducer.Op.PROD, e -> e.i));
        setupBag();
        assertEquals(max, bag.reduce(IntReducer.Op.MAX, e -> e.i));
        setupBag();
        assertEquals(min, bag.reduce(IntReducer.Op.MIN, e -> e.i));
    }

    @Test
    public void testReduceLong() {
        assertEquals(sum, chunkedList.reduce(LongReducer.Op.SUM, e -> e.l));
        assertEquals(prod, chunkedList.reduce(LongReducer.Op.PROD, e -> e.l));
        assertEquals(max, chunkedList.reduce(LongReducer.Op.MAX, e -> e.l));
        assertEquals(min, chunkedList.reduce(LongReducer.Op.MIN, e -> e.l));

        assertEquals(sum, bag.reduce(LongReducer.Op.SUM, e -> e.l));
        assertTrue(bag.isEmpty());
        setupBag();
        assertEquals(prod, bag.reduce(LongReducer.Op.PROD, e -> e.l));
        setupBag();
        assertEquals(max, bag.reduce(LongReducer.Op.MAX, e -> e.l));
        setupBag();
        assertEquals(min, bag.reduce(LongReducer.Op.MIN, e -> e.l));
    }

    @Test
    public void testReduceShort() {
        assertEquals(sum, chunkedList.reduce(ShortReducer.Op.SUM, e -> e.s));
        assertEquals(prod, chunkedList.reduce(ShortReducer.Op.PROD, e -> e.s));
        assertEquals(max, chunkedList.reduce(ShortReducer.Op.MAX, e -> e.s));
        assertEquals(min, chunkedList.reduce(ShortReducer.Op.MIN, e -> e.s));

        assertEquals(sum, bag.reduce(ShortReducer.Op.SUM, e -> e.s));
        assertTrue(bag.isEmpty());
        setupBag();
        assertEquals(prod, bag.reduce(ShortReducer.Op.PROD, e -> e.s));
        setupBag();
        assertEquals(max, bag.reduce(ShortReducer.Op.MAX, e -> e.s));
        setupBag();
        assertEquals(min, bag.reduce(ShortReducer.Op.MIN, e -> e.s));
    }
}
