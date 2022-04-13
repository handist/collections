package handist.collections;

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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@BenchmarkMode(Mode.AverageTime)
@State(Scope.Benchmark)
@Measurement(iterations = 5, time = 1)
@Warmup(iterations = 5, time = 1)
@Fork(1)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 10)
public class BenchSquareRange {

	public static void main(String[] args) throws RunnerException {
		final Options opt = new OptionsBuilder().include(BenchSquareRange.class.getSimpleName()).build();
		new Runner(opt).run();
	}

	@Benchmark
	public void benchmark() {

	}

	@Setup(Level.Invocation)
	public void setup() {

	}

	@TearDown(Level.Invocation)
	public void tearDown() {

	}
}
