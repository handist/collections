package handist.collections;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import apgas.impl.KryoSerializer;

/**
 * Test for SquareChunk (tentative)
 */
public class TestSquareChunk implements Serializable {

    /**
     * Class used as element in the tests. Have long values.
     */
    private class Element implements Serializable {
        private static final long serialVersionUID = -8082003165166213276L;
        public long outer;
        public long inner;
        public long value = 0;

        public Element(long outer, long inner) {
            this.outer = outer;
            this.inner = inner;
            this.value = outer * 10 + inner;
        }

        @Override
        public boolean equals(Object o) {
            final Element target = (Element) o;
            if (outer == target.outer && inner == target.inner) {
                return true;
            }
            return false;
        }

        @Override
        public String toString() {
            return "Element " + value + "(" + outer + "," + inner + ")";
        }
    }

    private static final long serialVersionUID = -1728899239222570223L;

    private SquareChunk<Element> squareChunk;
    private SquareRange squareRange;

    private final LongRange outerRange = new LongRange(1, 10);
    private final LongRange innerRange = new LongRange(2, 10);

    @Before
    public void setUp() {
        squareRange = new SquareRange(outerRange, innerRange);
        squareChunk = new SquareChunk<>(squareRange);
        for (long i = outerRange.from; i < outerRange.to; i++) {
            for (long j = innerRange.from; j < innerRange.to; j++) {
                squareChunk.set(i, j, new Element(i, j));
            }
        }
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testAsColumnList() {
        final RangedList<RangedList<Element>> columnList = squareChunk.asColumnList();
        columnList.forEach((columnIndex, column) -> {
            column.forEach((rowIndex, elem) -> {
                assertEquals(rowIndex, elem.outer);
                assertEquals(columnIndex, elem.inner);
            });
        });
    }

    @Test
    public void testAsRowList() {
        final RangedList<RangedList<Element>> rowList = squareChunk.asRowList();
        rowList.forEach((rowIndex, row) -> {
            row.forEach((columnIndex, elem) -> {
                assertEquals(rowIndex, elem.outer);
                assertEquals(columnIndex, elem.inner);
            });
        });
    }

    @Test
    public void testColumnIterator() {
        final RangedList<Element> columnView = squareChunk.getColumnView(5);
        final Iterator<Element> iter = columnView.iterator();
        while (iter.hasNext()) {
            assertEquals(5l, iter.next().inner);
        }
    }

    @Ignore
    @Test
    public void testColumnViewForEachImpl() {
        final RangedList<Element> columnView = squareChunk.getColumnView(5);
        columnView.forEachImpl(new LongRange(3, 7), (elem) -> {
            assertEquals(5l, elem.inner); // TODO : bug fix
            assertTrue((3 <= elem.outer && elem.outer < 7)); // TODO : bug fix
        });
        columnView.forEachImpl(new LongRange(3, 7), (i, elem) -> {
            assertEquals(5l, elem.inner); // TODO : bug fix
            assertEquals(i, elem.outer);
        });
        columnView.forEachImpl(new LongRange(3, 7), (elem, reciever) -> {
            reciever.accept(elem.inner);
        }, (Long i) -> {
            assertEquals(5l, (long) i);
        });
    }

    @Test
    public void testColumnViewGetRange() {
        final RangedList<Element> columnView = squareChunk.getColumnView(5);
        assertEquals(outerRange, columnView.getRange());
    }

    @Ignore
    @Test
    public void testColumnViewListIterator() {
        final RangedList<Element> columnView = squareChunk.getColumnView(5);
        final Iterator<Element> iter = columnView.listIterator();
        outerRange.forEach((Long index) -> {
            assertEquals(columnView.get(index), iter.next());
        });
    }

    @Test
    public void testColumnViewRangeCheck() {
        final RangedList<Element> columnView = squareChunk.getColumnView(5);
        columnView.rangeCheck(new LongRange(4, 6));
        try {
            columnView.rangeCheck(new LongRange(-1, 4));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
        try {
            columnView.rangeCheck(new LongRange(6, 11));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
        try {
            columnView.rangeCheck(new LongRange(-1, 11));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testConstructorWithArray() {
        // setup array
        final Element[] array = new Element[(int) squareRange.size()];
        final int[] count = { 0 };
        squareRange.forEach((i, j) -> {
            array[count[0]] = new Element(i, j);
            count[0]++;
        });
        // call constructor with array
        final SquareChunk<Element> sc = new SquareChunk<>(squareRange, array);
        // check every element
        sc.forEach((i, j, elem) -> {
            assertEquals(i * 10 + j, elem.value);
        });

        // test if throw exception when providing illegale size array.
        final Element[] a2 = { new Element(0, 0), new Element(1, 1) };
        try {
            new SquareChunk<>(squareRange, a2);
            throw new AssertionError();
        } catch (final IllegalArgumentException e) {
        }

        // test if throw exception when providing illegale type array.
        final Integer[] a3 = new Integer[(int) squareRange.size()];
        Arrays.fill(a3, 1);
        try {
            new SquareChunk<>(squareRange, a3);
        } catch (final IllegalArgumentException e) {
            throw new AssertionError();
        }
    }

    @Test
    public void testConstructorWithInitializer() {
        // call constructor with Function
        final SquareChunk<Element> sc = new SquareChunk<>(squareRange, (i, j) -> {
            return new Element(i, j);
        });
        // check every element
        sc.forEach((i, j, elem) -> {
            assertEquals(i * 10 + j, elem.value);
        });
    }

    @Test
    public void testConstructorWithValue() {
        // call constructor with value
        final SquareChunk<Element> sc = new SquareChunk<>(squareRange, new Element(1, 1));
        // check every element
        sc.forEach((elem) -> {
            assertEquals(11, elem.value);
        });
        // test if throw exception when providing illegale type array.
        try {
            new SquareChunk<>(squareRange, new Integer(0));
        } catch (final IllegalArgumentException e) {
            throw new AssertionError();
        }
    }

    @Test
    public void testContains() {
        assertTrue(squareChunk.contains(new Element(1, 2)));
        assertTrue(squareChunk.contains(new Element(9, 9)));
        assertFalse(squareChunk.contains(new Element(0, 1)));
        assertFalse(squareChunk.contains(new Element(10, 10)));
        assertFalse(squareChunk.contains(new Element(5, 10)));
        assertFalse(squareChunk.contains(new Element(10, 5)));
    }

    @Test
    public void testEquals() {
        final SquareChunk<Element> copy = squareChunk;
        assertTrue(copy.equals(squareChunk));

        final SquareChunk<Element> others = new SquareChunk<>(squareRange, new Element(0, 0));
        assertFalse(others.equals(squareChunk));

        final SquareChunk<Element> sameElems = new SquareChunk<>(squareRange, (i, j) -> {
            return new Element(i, j);
        });
        assertTrue(sameElems.equals(squareChunk));
    }

    @Test
    public void testForEach() {
        squareChunk.forEach((elem) -> {
            elem.value += 5;
        });
        squareRange.forEach((i, j) -> {
            assertEquals(i * 10 + j + 5, squareChunk.get(i, j).value);
        });
    }

    @Test
    public void testForEachColumn() {
        final int[] count = { 0 }; // count number of iterations
        // check if it iterates correctly
        squareChunk.forEachColumn((long column, RangedList<Element> list) -> {
            list.forEach((Element elem) -> {
                assertEquals(column, elem.inner);
                count[0]++;
            });
        });
        assertEquals(outerRange.size() * innerRange.size(), count[0]);
    }

    @Test
    public void testForEachColumnWithRange() {
        final int[] count = { 0 }; // count number of iterations
        // check if it iterates correctly
        final LongRange range = new LongRange(5, 7);
        squareChunk.forEachColumn(range, (long column, RangedList<Element> list) -> {
            list.forEach((elem) -> {
                assertEquals(column, elem.inner);
                count[0]++;
                if (column < 5 || column >= 7) {
                    throw new AssertionError();
                }
            });
        });
        assertEquals(outerRange.size() * range.size(), count[0]);
    }

    @Test
    public void testForEachRow() {
        final int[] count = { 0 }; // count number of iterations
        // check if it iterates correctly
        squareChunk.forEachRow((long row, RangedList<Element> list) -> {
            list.forEach((elem) -> {
                assertEquals(row, elem.outer);
                count[0]++;
            });
        });
        assertEquals(outerRange.size() * innerRange.size(), count[0]);
    }

    @Test
    public void testForEachRowWithRange() {
        final int[] count = { 0 }; // count number of iterations
        final LongRange range = new LongRange(5, 7);
        // check if it iterates correctly
        squareChunk.forEachRow(range, (long row, RangedList<Element> list) -> {
            list.forEach((elem) -> {
                assertEquals(row, elem.outer);
                count[0]++;
                if (row < 5 || row >= 7) {
                    throw new AssertionError();
                }
            });
        });
        assertEquals(range.size() * innerRange.size(), count[0]);
    }

    @Test
    public void testForEachWithIndex() {
        squareChunk.forEach((long i, long j, Element elem) -> {
            elem.value = j * 10 + i;
        });
        squareRange.forEach((i, j) -> {
            assertEquals(j * 10 + i, squareChunk.get(i, j).value);
        });
    }

    @Test
    public void testForEachWithRange() {
        final SquareRange range = new SquareRange(new LongRange(5, 7), new LongRange(5, 7));
        squareChunk.forEach(range, (elem) -> {
            elem.value += 5;
        });
        squareRange.forEach((i, j) -> {
            if (range.contains(i, j)) {
                assertEquals(i * 10 + j + 5, squareChunk.get(i, j).value);
            } else {
                assertEquals(i * 10 + j, squareChunk.get(i, j).value);
            }
        });
    }

    @Test
    public void testForEachWithRangeIndex() {
        final SquareRange range = new SquareRange(new LongRange(5, 7), new LongRange(5, 7));
        squareChunk.forEach(range, (long i, long j, Element elem) -> {
            elem.value = j * 10 + i;
        });
        for (long i = outerRange.from; i < outerRange.to; i++) {
            for (long j = innerRange.from; j < innerRange.to; j++) {
                if (range.contains(i, j)) {
                    assertEquals(j * 10 + i, squareChunk.get(i, j).value);
                } else {
                    assertEquals(i * 10 + j, squareChunk.get(i, j).value);
                }
            }
        }
    }

    @Test
    public void testForEachWithSiblings() {
        final SquareRange range = new SquareRange(new LongRange(5, 7), new LongRange(5, 7));
        squareChunk.forEachWithSiblings(range, (siblings) -> {
            siblings.put(new Element(-1, -1));
        });
        for (long i = outerRange.from; i < outerRange.to; i++) {
            for (long j = innerRange.from; j < innerRange.to; j++) {
                if (range.contains(i, j)) {
                    assertEquals(-1, squareChunk.get(i, j).outer);
                } else {
                    assertEquals(i, squareChunk.get(i, j).outer);
                }
            }
        }
    }

    @Test
    public void testGetAndException() {
        try {
            squareChunk.get(-1, 5);
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
        try {
            squareChunk.get(5, -1);
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testGetColumnView() {
        final RangedList<Element> columnView = squareChunk.getColumnView(5);
        columnView.forEach((i, elem) -> {
            assertEquals(5, elem.inner);
            assertEquals(i, elem.outer);
        });
        try {
            squareChunk.getColumnView(-1);
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testGetRowView() {
        final RangedList<Element> rowView = squareChunk.getRowView(5);
        rowView.forEach((i, elem) -> {
            assertEquals(5, elem.outer);
            assertEquals(i, elem.inner);
        });
        try {
            squareChunk.getRowView(-1);
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testIterator() {
        final Iterator<Element> iter = squareChunk.iterator();
        squareRange.forEach((i, j) -> {
            assertEquals(squareChunk.get(i, j), iter.next());
        });
        assertFalse(iter.hasNext());
    }

    @Test
    public void testRowViewForEachImpl() {
        final RangedList<Element> rowView = squareChunk.getRowView(5);
        rowView.forEachImpl(new LongRange(3, 7), (elem) -> {
            assertEquals(5l, elem.outer);
            assertTrue((3 <= elem.inner && elem.inner < 7));
        });
        rowView.forEachImpl(new LongRange(3, 7), (i, elem) -> {
            assertEquals(5l, elem.outer);
            assertEquals(i, elem.inner);
        });
        rowView.forEachImpl(new LongRange(3, 7), (elem, reciever) -> {
            reciever.accept(elem.outer);
        }, (Long i) -> {
            assertEquals(5l, (long) i);
        });
    }

    @Test
    public void testRowViewGetRange() {
        final RangedList<Element> rowView = squareChunk.getRowView(5);
        assertEquals(innerRange, rowView.getRange());
    }

    @Test
    public void testRowViewIterator() {
        final Iterator<Element> iter = squareChunk.getRowView(5).iterator();
        innerRange.forEach((Long i) -> {
            final Element elem = iter.next();
            assertEquals((long) i, elem.inner);
            assertEquals(5l, elem.outer);
        });
    }

    @Test
    public void testRowViewRangeCheck() {
        final RangedList<Element> rowView = squareChunk.getRowView(5);
        rowView.rangeCheck(new LongRange(4, 6));
        try {
            rowView.rangeCheck(new LongRange(-1, 4));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
        try {
            rowView.rangeCheck(new LongRange(6, 11));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
        try {
            rowView.rangeCheck(new LongRange(-1, 11));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testSetAndException() {
        try {
            squareChunk.set(-1, 5, new Element(0, 0));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
        try {
            squareChunk.set(5, -1, new Element(0, 0));
            throw new AssertionError();
        } catch (final IndexOutOfBoundsException e) {
        }
    }

    @Test
    public void testSetupFrom() {
        final SquareChunk<Long> sc = new SquareChunk<>(squareRange);
        sc.setupFrom(squareChunk, (Element elem) -> {
            return elem.value;
        });
        for (long i = outerRange.from; i < outerRange.to; i++) {
            for (long j = innerRange.from; j < innerRange.to; j++) {
                assertEquals(i * 10 + j, (long) sc.get(i, j));
            }
        }
    }

    @Test
    public void testSubIterator() {
        final SquareRange range = new SquareRange(new LongRange(4, 6), new LongRange(4, 6));
        final Iterator<Element> iter = squareChunk.subIterator(range);
        range.forEach((i, j) -> {
            assertEquals(squareChunk.get(i, j), iter.next());
        });
        assertFalse(iter.hasNext());
    }

    @Test
    public void testSubView() {
        final SquareRange range = new SquareRange(new LongRange(5, 7), new LongRange(5, 7));
        final SquareRangedList<Element> view = squareChunk.subView(range);
        final int[] count = { 0 };
        view.forEach((i, j, elem) -> {
            count[0]++;
            assertEquals(i * 10 + j, view.get(i, j).value);
        });
        assertEquals(4, count[0]);
    }

    @Test
    public void testToArray() {
        final Object[] array = squareChunk.toArray();
        final Iterator<Element> iter = squareChunk.iterator();
        for (final Object elem : array) {
            assertEquals(iter.next(), elem);
        }
    }

    @Test
    public void testToArrayWithRange() {
        final SquareRange r = new SquareRange(new LongRange(4, 6), new LongRange(4, 6));
        final Object[] array = squareChunk.toArray(r);
        final Iterator<Element> iter = squareChunk.subIterator(r);
        for (final Object elem : array) {
            assertEquals(iter.next(), elem);
        }
    }

    @Test
    public void testToChunkWithRange() {
        final SquareRange r = new SquareRange(new LongRange(4, 6), new LongRange(4, 6));
        final SquareChunk<Element> c = squareChunk.toChunk(r);
        final Iterator<Element> iter = squareChunk.subIterator(r);
        for (final Object elem : c) {
            assertEquals(iter.next(), elem);
        }
    }

    @Test
    public void testToListWithRange() {
        final SquareRange r = new SquareRange(new LongRange(4, 6), new LongRange(4, 6));
        final List<Element> list = squareChunk.toList(r);
        final Iterator<Element> iter = squareChunk.subIterator(r);
        for (final Object elem : list) {
            assertEquals(iter.next(), elem);
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteAndRead() throws IOException, ClassNotFoundException {
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bOut);
        out.writeObject(squareChunk);
        out.close();

        final ByteArrayInputStream bIn = new ByteArrayInputStream(bOut.toByteArray());
        final ObjectInputStream in = new ObjectInputStream(bIn);
        final SquareChunk<Element> newChunk = (SquareChunk<Element>) in.readObject();
        in.close();

        squareRange.forEach((i, j) -> {
            assertEquals(squareChunk.get(i, j), newChunk.get(i, j));
        });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testWriteAndReadKryo() {
        final Kryo kryo1 = KryoSerializer.getKryoInstance();
        final ByteArrayOutputStream bOut = new ByteArrayOutputStream();
        final Output out = new Output(bOut);
        kryo1.writeClassAndObject(out, squareChunk);
        out.close();

        final Kryo kryo2 = KryoSerializer.getKryoInstance();
        final ByteArrayInputStream bIn = new ByteArrayInputStream(bOut.toByteArray());
        final Input in = new Input(bIn);
        final SquareChunk<Element> newChunk = (SquareChunk<Element>) kryo2.readClassAndObject(in);
        in.close();

        squareRange.forEach((i, j) -> {
            assertEquals(squareChunk.get(i, j), newChunk.get(i, j));
        });
    }

//    @Test
//    public void testMatrixMul() {
//        final LongRange rangeAx = new LongRange(5, 25);
//        final LongRange rangeAy = new LongRange(10, 15);
//
//        final LongRange rangeBy = rangeAx;
//        final LongRange rangeBx = new LongRange(10, 20);
//        final SquareChunk<Long> matrixA = new SquareChunk<>(new SquareRange(rangeAy, rangeAx), (long y, long x) -> {
//            return y;
//        });
//        final SquareChunk<Long> matrixB = new SquareChunk<>(new SquareRange(rangeBy, rangeBx), (long y, long x) -> {
//            return x;
//        });
//        matrixA.debugPrint("tagA");
//        matrixB.debugPrint("tagB");
//
//        final SquareChunk<Long> matrixC = new SquareChunk<>(new SquareRange(rangeAy, rangeBx));
//        matrixC.debugPrint("tagC");
//
//        matrixA.forEachRow((long y, RangedList<Long> row) -> {
//            matrixB.forEachColumn((long x, RangedList<Long> column) -> {
//                final Long val = row.reduce(column, (Long a, Long b) -> {
//                    return a * b;
//                }, 0L, (Long sum, Long v) -> {
//                    return sum + v;
//                });
//
//                matrixC.set(y, x, val);
//            });
//        });
//        // maybe there are many ways to write matrix...
//        matrixC.debugPrint("tagCX");
//
//        final SquareChunk<Long> matrixC2 = new SquareChunk<>(new SquareRange(rangeAy, rangeBx));
//        final RangedList<RangedList<Long>> Arows = matrixA.asRowList();
//        final RangedList<RangedList<Long>> Bcols = matrixB.asColumnList();
//        matrixC2.asRowList().map(rangeAy, Arows, (RangedList<Long> Crow, RangedList<Long> Arow) -> {
//            System.out.println("pp:" + rangeAx + ":" + Bcols.getRange());
//            Crow.setupFrom(rangeBx, Bcols, (RangedList<Long> Bcol) -> {
//                return Arow.reduce(Bcol, (Long a, Long b) -> {
//                    return a * b;
//                }, 0L, (Long sum, Long diff) -> {
//                    return sum + diff;
//                });
//            });
//        });
//        matrixC2.debugPrint("tagC2");
//
//        for (final long i : matrixC.getRange().outer) {
//            for (final long j : matrixC.getRange().inner) {
//                assertEquals(matrixC.get(i, j), matrixC2.get(i, j));
//            }
//        }
//    }
//
//    @Test
//    public void testMisc1() {
//        final SquareRange rangeX = new SquareRange(new LongRange(100, 110), new LongRange(10, 20));
//        final SquareChunk<String> chunkXstr = new SquareChunk<>(rangeX, (long i1, long i2) -> {
//            return "[" + i1 + ":" + i2 + "]";
//        });
//        chunkXstr.forEach((String str) -> {
//            System.out.print(str);
//        });
//        System.out.println();
//        chunkXstr.forEach((long first, long second, String str) -> {
//            System.out.println("[" + first + "," + second + ":" + str + "]");
//        });
//        final SquareRange rangeY = new SquareRange(new LongRange(102, 105), new LongRange(12, 15));
//        chunkXstr.forEachWithSiblings(rangeY, (SquareSiblingAccessor<String> acc) -> {
//            System.out.println("SIB[" + acc.get(0, 0) + "::" + acc.get(0, -1) + ":" + acc.get(0, 1) + "^"
//                    + acc.get(-1, 0) + "_" + acc.get(1, 0) + "]");
//        });
//
//        chunkXstr.forEachRow((long row, RangedList<String> rowView) -> {
//            final long start = row - 89;
//            final long to = 18;
//            if (start >= to) {
//                return;
//            }
//            final LongRange scan = new LongRange(start, to);
//            System.out.println("row iter:" + row + "=>" + scan);
//            rowView.forEach(scan, (long column, String e) -> {
//                System.out.print("(" + column + ":" + e + ")");
//            });
//            System.out.println();
//        });
//
//        chunkXstr.forEachColumn((long column, RangedList<String> columnView) -> {
//            final long start = 101;
//            final long to = column + 89;
//            if (start >= to) {
//                return;
//            }
//            final LongRange scan = new LongRange(start, to);
//            System.out.println("column iter:" + column + "=>" + scan);
//            columnView.forEach(scan, (long row, String e) -> {
//                System.out.print("(" + row + ":" + e + ")");
//            });
//            System.out.println();
//        });
//
//        final SquareChunk<Long> matrixX = new SquareChunk<>(rangeX, (long i1, long i2) -> {
//            return i1 * 1000 + i2;
//        });
//        matrixX.debugPrint("matrixX");
//        final SquareChunk<Long> matrixY = new SquareChunk<>(rangeY, (long i1, long i2) -> {
//            return i1 * 2000 + i2 * 2;
//        });
//        matrixY.debugPrint("matrixY");
//        matrixX.setupFrom(matrixY, (Long x) -> {
//            return x + 70000000;
//        });
//        matrixX.debugPrint("matrixX2");
//        matrixX.getRowView(100).setupFrom(matrixY.getRowView(103), (Long x) -> x);
//        matrixX.getColumnView(11).setupFrom(matrixY.getColumnView(13), (Long x) -> x);
//        matrixX.debugPrint("matrixX3");
//    }

}
