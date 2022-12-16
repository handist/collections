package handist.collections.reducer;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import handist.collections.Chunk;
import handist.collections.LongRange;
import handist.collections.dist.DistChunkedList;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * This test class is here to test the correct execution of larger-scale object
 * reduction.
 * <p>
 * Depending on the objects used and the number of hosts, some reductions based
 * on custom objects fail to compute when the entire reduction is conducted on a
 * single host. As a result, we modified our simplistic implementation to
 * construct a proper reduction tree. This test is here to confirm that this new
 * implementation indeed solves the issue.
 * <p>
 * This test is based on a K-Means case.
 *
 * @author Patrick Finnerty
 *
 */
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 4, launcher = TestLauncher.class)
public class IT_LargeScaleReduction implements Serializable {

    /**
     * Reduction to perform as part of the K-Means computation
     */
    final public class AveragePosition extends Reducer<AveragePosition, Point> {

        /** Serial Version UID */
        private static final long serialVersionUID = 29050701329215796L;

        /**
         * First index: cluster id Second index: position in the 'nth' dimension of the
         * point
         */
        public final double[][] clusterCenters;

        /**
         * Number of points contained in the average position contained in
         * {@link #clusterCenters}
         */
        final long[] includedPoints;

        /**
         * Constructor
         *
         * @param k number of clusters considered for computation
         */
        public AveragePosition(int k, int dimension) {
            clusterCenters = new double[k][dimension];
            includedPoints = new long[k];
        }

        @Override
        public void merge(AveragePosition reducer) {
            // Compute the average of this and reducer

            // For each 'k' centroid
            for (int k = 0; k < clusterCenters.length; k++) {
                // For each dimension 'd' of the centroid
                for (int d = 0; d < clusterCenters[k].length; d++) {
                    clusterCenters[k][d] = weightedAverage(clusterCenters[k][d], includedPoints[k],
                            reducer.clusterCenters[k][d], reducer.includedPoints[k]);
                }
                includedPoints[k] += reducer.includedPoints[k];
            }
        }

        @Override
        public AveragePosition newReducer() {
            return new AveragePosition(clusterCenters.length, clusterCenters[0].length);
        }

        @Override
        public void reduce(Point input) {
            final int k = input.clusterAssignment;
            for (int d = 0; d < clusterCenters[k].length; d++) {
                clusterCenters[k][d] = weightedAverage(clusterCenters[k][d], includedPoints[k], input.position[d], 1l);
            }
            includedPoints[k]++;
        }
    }

    final public class ClosestPoint extends Reducer<ClosestPoint, Point> {

        /** Serial Version UID */
        private static final long serialVersionUID = -5053187857859985586L;

        public final double[][] closestPointCoordinates;
        final double[][] clusterAverage;
        final double[] distanceToAverage;

        /**
         * Constructor
         *
         * @param k         number of clusters considered for computation
         * @param dimension dimension of the points used
         */
        public ClosestPoint(int k, int dimension, double[][] clusterAveragePositions) {
            closestPointCoordinates = new double[k][dimension];
            distanceToAverage = new double[k];
            for (int l = 0; l < k; l++) {
                distanceToAverage[l] = Double.MAX_VALUE;
            }
            this.clusterAverage = clusterAveragePositions;
        }

        @Override
        public void merge(ClosestPoint reducer) {
            // For each cluster ...
            for (int k = 0; k < distanceToAverage.length; k++) {
                // Check if "reducer" found a closer point than this instance
                if (reducer.distanceToAverage[k] < distanceToAverage[k]) {
                    // The reducer has a point closer than the prl held by this instance
                    closestPointCoordinates[k] = reducer.closestPointCoordinates[k];
                    distanceToAverage[k] = reducer.distanceToAverage[k];
                }
            }
        }

        @Override
        public ClosestPoint newReducer() {
            return new ClosestPoint(distanceToAverage.length, clusterAverage[0].length, clusterAverage);
        }

        @Override
        public void reduce(Point input) {
            final double distance = distance(input.position, clusterAverage[input.clusterAssignment]);
            if (distance < distanceToAverage[input.clusterAssignment]) {
                distanceToAverage[input.clusterAssignment] = distance;
                for (int n = 0; n < input.position.length; n++) {
                    closestPointCoordinates[input.clusterAssignment][n] = input.position[n];
                }
            }
        }

    }

    /**
     * Class containing n-dimensional points
     */
    public final class Point implements Serializable {
        /** Generated Serial Version UID */
        private static final long serialVersionUID = 903981107365981546L;

        /** Cluster id this point belongs to */
        int clusterAssignment;

        /** Coordinates array */
        public final double[] position;

        /**
         * Constructor
         *
         * @param initialPosition array of {@link Double} containing the point
         *                        coordinates of this point
         */
        public Point(Double[] initialPosition) {
            position = new double[initialPosition.length];
            for (int i = 0; i < position.length; i++) {
                position[i] = initialPosition[i];
            }
        }

        public void assignCluster(double[][] clusterCentroids) {
            double closestDistance = Double.MAX_VALUE;
            for (int i = 0; i < clusterCentroids.length; i++) {
                final double distance = distance(position, clusterCentroids[i]);
                if (distance < closestDistance) {
                    closestDistance = distance;
                    clusterAssignment = i;
                }
            }
        }
    }

    /** Serial Version UID */
    private static final long serialVersionUID = 4056531390475545633L;

    /*
     * Constants setting the size of the problem
     */
    private static final int CHUNKCOUNT = 100;
    private static final int CHUNKSIZE = 100;
    private static final int DATASIZE = CHUNKCOUNT * CHUNKSIZE;
    private static final int DIMENSION = 5;
    private static final int K = 1000; // TODO This makes objects too large for the current implementation to handle

    /**
     * Euclidean distance calculation between n-dimensional coordinates. The square
     * root is not applied to the sum of the squares as it preserves the order.
     *
     * @param a first point
     * @param b second point
     * @return the distance between the two points.
     */
    static double distance(double[] a, double[] b) {
        double result = 0.0;
        for (int i = 0; i < a.length; i++) {
            final double diff = a[i] - b[i];
            result += diff * diff;
        }
        return result;
    }

    /**
     * Method taken from the Renaissance JavaKMeans benchmark
     *
     * @param count        number of data points desired
     * @param dimension    number of dimensions of data points
     * @param clusterCount "K" in K-Means
     * @return a list of Double arrays, each array representing a data point
     */
    static List<Double[]> generateData(final int count, final int dimension, final int clusterCount) {
        // Create random generators for individual dimensions.
        final Random[] randoms = IntStream.range(0, dimension).mapToObj(d -> new Random(1 + 2 * d))
                .toArray(Random[]::new);

        // Generate random data for all dimensions.
        return IntStream.range(0, count).mapToObj(i -> {
            return IntStream.range(0, dimension).mapToObj(
                    d -> (((i + (1 + 2 * d)) % clusterCount) * 1.0 / clusterCount) + randoms[d].nextDouble() * 0.5)
                    .toArray(Double[]::new);
        }).collect(Collectors.toList());
    }

    /**
     * Selects some random points to be the initial centroids
     *
     * @param sampleCount number of centroids desired
     * @param data        the data from which to sample the initial centroids
     * @param random      random generator
     * @return a list of initial centroids
     */
    static List<Double[]> randomSample(final int sampleCount, final List<Double[]> data, final Random random) {
        return random.ints(sampleCount, 0, data.size()).mapToObj(data::get).collect(Collectors.toList());
    }

    static double weightedAverage(double a, long includedPoints, double b, long includedPoints2) {
        return ((a * includedPoints) + (b * includedPoints2)) / (includedPoints + includedPoints2);
    }

    DistChunkedList<Point> points;
    List<Double[]> initialCentroids;
    double[][] initialClusterCenter;

    @Before
    public void before() {
        final TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();
        final Random r = new Random();
        points = new DistChunkedList<>();
        final List<Double[]> initialPoints = generateData(DATASIZE, DIMENSION, K);
        initialCentroids = randomSample(K, initialPoints, r);

        world.broadcastFlat(() -> {
            final long offset = world.rank() * CHUNKCOUNT * CHUNKSIZE;
            final List<Double[]> pts = generateData(DATASIZE, DIMENSION, K);
            for (int chunkNumber = 0; chunkNumber < CHUNKCOUNT; chunkNumber++) {
                final LongRange chunkRange = new LongRange(chunkNumber * CHUNKSIZE + offset,
                        (chunkNumber + 1) * CHUNKSIZE + offset);
                final Chunk<Point> c = new Chunk<>(chunkRange, l -> {
                    return new Point(pts.get((int) (l - offset)));
                });
                points.add(c);
            }
        });

        // We convert the list of initial centroids to a 2Darray of double
        initialClusterCenter = new double[K][DIMENSION];
        for (int i = 0; i < K; i++) {
            for (int j = 0; j < DIMENSION; j++) {
                initialClusterCenter[i][j] = initialCentroids.get(i)[j];
            }
        }
    }

    @Test(timeout = 15000)
    public void testLargeReduction() {
        final TeamedPlaceGroup world = TeamedPlaceGroup.getWorld();

        // Perform one K-Means iteration
        world.broadcastFlat(() -> {
            final double[][] clusterCentroids = initialClusterCenter;
            points.parallelForEach(p -> p.assignCluster(clusterCentroids));
            // Reduction happens on the line below
            final AveragePosition avgP = points.team().parallelReduce(new AveragePosition(K, DIMENSION));
            final ClosestPoint cldP = points.team().parallelReduce(new ClosestPoint(K, DIMENSION, avgP.clusterCenters));

            initialClusterCenter = cldP.closestPointCoordinates;
        });
    }
}
