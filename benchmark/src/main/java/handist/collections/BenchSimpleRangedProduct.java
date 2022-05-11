package handist.collections;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Measurement(iterations = 10, time = 1)
@Warmup(iterations = 5, time = 1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 10)
public class BenchSimpleRangedProduct {

    /** The class for the inner rangedlist element */
    private static class Inner {
        int v;

        public Inner(int v) {
            this.v = v;
        }
    }

    /** The class for the outer rangedlist element */
    private static class Outer {
        int v;

        public Outer(int v) {
            this.v = v;
        }
    }

    public static void main(String[] args) throws RunnerException {
        final Class<BenchSimpleRangedProduct> c = BenchSimpleRangedProduct.class;

        final Options opt = new OptionsBuilder().include(c.getSimpleName()).result("results/" + c.getSimpleName())
                .build();
        new Runner(opt).run();
    }

    private RangedProduct<Inner, Outer> squareProd;
    private RangedProduct<Inner, Outer> triangleProd;
    private RangedList<Inner> innerList;
    private RangedList<Outer> outerList;
    private final LongRange innerRange = new LongRange(0, 10000);
    private final LongRange outerRange = new LongRange(0, 10000);

    private final int seed = 12345;

    @Setup(Level.Iteration)
    public void setup() {
        final Random r = new Random(seed);
        innerList = new Chunk<>(innerRange, (i) -> {
            return new Inner(r.nextInt());
        });
        outerList = new Chunk<>(outerRange, (i) -> {
            return new Outer(r.nextInt());
        });
        squareProd = RangedProduct.newProd(innerList, outerList);
        triangleProd = RangedProduct.newProd(innerList, outerList);
    }

    @TearDown(Level.Iteration)
    public void tearDown() {
        squareProd = null;
        triangleProd = null;
        innerList = null;
        outerList = null;
    }

    @Benchmark
    public void testForEach_Square(Blackhole result) {
        final long[] sum = { 0 };
        squareProd.forEach((pair) -> {
            sum[0] += pair.first.v + pair.second.v;
        });
        result.consume(sum[0]);
    }

    @Benchmark
    public void testForEach_Triangle(Blackhole result) {
        final long[] sum = { 0 };
        triangleProd.forEach((pair) -> {
            sum[0] += pair.first.v + pair.second.v;
        });
        result.consume(sum[0]);
    }

    @Benchmark
    public void testSplit_Square(Blackhole result) {
        for (int i = 0; i < 1000; i++) {
            final RangedProductList<Inner, Outer> split = squareProd.split(100, 100);
            result.consume(split);
        }
    }

    @Benchmark
    public void testSplit_Triangle(Blackhole result) {
        for (int i = 0; i < 1000; i++) {
            final RangedProductList<Inner, Outer> split = triangleProd.split(100, 100);
            result.consume(split);
        }
    }
}
