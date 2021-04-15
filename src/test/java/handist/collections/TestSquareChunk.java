package handist.collections;

import handist.collections.function.LongTBiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

import static org.junit.Assert.*;

/**
 * Test for SquareChunk (tentative)
 */
public class TestSquareChunk {
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testMisc1() {
        SquareRange rangeX =
                new SquareRange(new LongRange(100, 110), new LongRange(10, 20));
        SquareChunk<String> chunkXstr =
                new SquareChunk<>(rangeX, (long i1, long i2) -> {
                    return "[" + i1 + ":" + i2 + "]";
                });
        chunkXstr.forEach((String str) -> {
            System.out.print(str);
        });
        System.out.println();
        chunkXstr.forEach((long first, long second, String str) -> {
            System.out.println("[" + first + "," + second + ":" + str + "]");
        });
        SquareRange rangeY =
                new SquareRange(new LongRange(102, 105), new LongRange(12, 15));
        chunkXstr.forEach(rangeY, (SquareSiblingAccessor<String> acc) -> {
            System.out.println("SIB[" + acc.get(0, 0) + "::"
                    + acc.get(0, -1) + ":" + acc.get(0, 1) + "^" +
                    acc.get(-1, 0) + "_" + acc.get(1, 0) + "]");
        });

        chunkXstr.forEachRow((long row, RangedList<String> rowView) -> {
            long start = row - 89;
            long to = 18;
            if (start >= to) return;
            LongRange scan = new LongRange(start, to);
            System.out.println("row iter:" + row + "=>" + scan);
            rowView.forEach(scan, (long column, String e) -> {
                System.out.print("(" + column + ":" + e + ")");
            });
            System.out.println();
        });

        chunkXstr.forEachColumn((long column, RangedList<String> columnView) -> {
            long start = 101;
            long to = column + 89;
            if (start >= to) return;
            LongRange scan = new LongRange(start, to);
            System.out.println("column iter:" + column + "=>" + scan);
            columnView.forEach(scan, (long row, String e) -> {
                System.out.print("(" + row + ":" + e + ")");
            });
            System.out.println();
        });

        SquareChunk<Long> matrixX =
                new SquareChunk<>(rangeX, (long i1, long i2) -> {
                    return i1 * 1000 + i2;
                });
        matrixX.debugPrint("matrixX");
        SquareChunk<Long> matrixY =
                new SquareChunk<>(rangeY, (long i1, long i2) -> {
                    return i1 * 2000 + i2 * 2;
                });
        matrixY.debugPrint("matrixY");
        matrixX.setupFrom(matrixY, (Long x) -> {
            return x + 70000000;
        });
        matrixX.debugPrint("matrixX2");
        matrixX.getRowView(100).setupFrom(matrixY.getRowView(103), (Long x) -> x);
        matrixX.getColumnView(11).setupFrom(matrixY.getColumnView(13), (Long x) -> x);
        matrixX.debugPrint("matrixX3");
    }

    @Test
    public void testMatrixMul() {
        LongRange rangeAx = new LongRange(5,25);
        LongRange rangeAy = new LongRange(10,15);

        LongRange rangeBy = rangeAx;
        LongRange rangeBx = new LongRange(10, 20);
        SquareChunk<Long> matrixA = new SquareChunk<>(new SquareRange(rangeAy, rangeAx), (long y, long x)->{
            return y;
        });
        SquareChunk<Long> matrixB = new SquareChunk<>(new SquareRange(rangeBy, rangeBx), (long y, long x)->{
            return x;
        });
        matrixA.debugPrint("tagA");
        matrixB.debugPrint("tagB");

        SquareChunk<Long> matrixC = new SquareChunk<>(new SquareRange(rangeAy, rangeBx));
        matrixC.debugPrint("tagC");

        matrixA.forEachRow((long y, RangedList<Long> row)->{
            matrixB.forEachColumn((long x, RangedList<Long> column)->{
                Long val = row.reduce(column, (Long a, Long b)->{ return a*b; },
                        0L, (Long sum, Long v)->{ return sum+v;});

                matrixC.set(y, x, val);
            });
        });
        // maybe there are many ways to write matrix...
        matrixC.debugPrint("tagCX");

        SquareChunk<Long> matrixC2 = new SquareChunk<>(new SquareRange(rangeAy, rangeBx));
        RangedList<RangedList<Long>> Arows = matrixA.asRowList();
        RangedList<RangedList<Long>> Bcols = matrixB.asColumnList();
        matrixC2.asRowList().map(rangeAy, Arows, (RangedList<Long> Crow, RangedList<Long> Arow)-> {
            System.out.println("pp:"+rangeAx+":"+Bcols.getRange());
            Crow.setupFrom(rangeBx, Bcols, (RangedList<Long> Bcol)->{
                return Arow.reduce(Bcol, (Long a, Long b)->{return a*b;}, 0L, (Long sum, Long diff)->{
                    return sum+diff;
                });
            });
        });
        matrixC2.debugPrint("tagC2");

        for(long i:matrixC.getRange().outer) {
            for(long j: matrixC.getRange().inner) {
                assertEquals(matrixC.get(i, j), matrixC2.get(i, j));
            }
        }


    }


}
