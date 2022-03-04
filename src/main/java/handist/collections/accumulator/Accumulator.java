package handist.collections.accumulator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import handist.collections.ChunkedList;
import handist.collections.LongRange;

/**
 * Class representing a factory for accumulator instances dedicated to a single
 * thread.
 * <p>
 * This allows for multiple threads to record information in a thread-dedicated
 * {@link ChunkedList}&lt;A&gt;, with each A object "accumulating" information
 * with regards to the "T" object at the matching index in the target
 * collection.
 * <p>
 * Computation with an accumulator takes place in 3 phases:
 * <ol>
 * <li>An {@link Accumulator} with a certain accumulator-allocation policy is
 * created. the type A used to accumulate information and how to initialize new
 * instance of this A type are specified to the constructor.
 * <li>This accumulator is used in one or multiple accumulation calls where the
 * information contained in some collection is used as the source to register
 * information into the accumulator objects contained by the {@link Accumulator}
 * <li>The information accumulated in the {@link Accumulator} is used to update
 * some collection's elements using method
 * {@link ChunkedList#accept(Accumulator, java.util.function.BiConsumer)} or
 * {@link ChunkedList#parallelAccept(Accumulator, java.util.function.BiConsumer)}.
 * The function which determines how to update individual T elements of the
 * target collection based on the information contained in the A accumulators is
 * specified as parameter to these functions.
 * </ol>
 * <p>
 * There is currently one implementation of {@link Accumulator}:
 * <ul>
 * <li>{@link AccumulatorCompleteRange} which prepares accumulators for all the
 * ranges it is given at the time of construction. Use this variant if you know
 * that the elements processed during the accumulation phase will need to record
 * information about (almost) all indices contained in the target
 * {@link ChunkedList}&lt;T&gt;.
 * </ul>
 *
 * @author Kawanishi Yoshiki
 *
 * @param <A> the "accumulator" type used to keep information about the
 *            modification to be performed on the target {@link ChunkedList}
 */
public abstract class Accumulator<A> {

    /**
     * ThreadLocalAccumulator provided as parameter when accumulating information
     * inside accumulators. This interface provides access to the individual
     * accumulators of type A.
     * <p>
     * Internally, various accumulator allocation strategies can be used (on-demand,
     * fixed initialization on specific ranges defined in advance) and are
     * implemented into the specializations of class {@link Accumulator}.
     *
     * @author Patrick Finnerty
     *
     * @param <A> the type of the accumulator used to collect data
     */
    public interface ThreadLocalAccumulator<A> {

        /**
         * Provides the accumulator instance dedicated to this thread for the specified
         * index. This method may throw Exceptions if the allocation contract of the
         * underlying {@link Accumulator} is not respected.
         *
         * @param idx index of the accumulator desired
         * @return the accumulator object for this index
         */
        public A acquire(long idx);

        /**
         * Obtains the accumulators for the range specified by the parameter.
         * <p>
         * This method may throw an {@link Exception} if the allocation contract of the
         * underlying {@link Accumulator} is not respected.
         *
         * @param range the range of accumulators to obtain from the thread-local
         *              accumulator
         * @return ranged list of accumulators containing the specified range
         */
        public ChunkedList<A> acquire(LongRange range);

        /**
         * Returns the underlying {@link ChunkedList} containing the accumulators
         * <p>
         * Internal-use only. Call at your own peril.
         *
         * @return the underlying {@link ChunkedList} containing the accumulators of
         *         this {@link ThreadLocalAccumulator}
         */
        public ChunkedList<A> getChunkedList();

        /**
         * Returns the ranges that the current thread-local accumulator has internally
         * initialized.
         *
         * @return collection of {@link LongRange} present in this accumulator
         */
        public Collection<LongRange> ranges();
    }

    /**
     * Collection which keeps track of the {@link ChunkedList} created for each
     * thread participating in the computation.
     */
    protected final ArrayList<ThreadLocalAccumulator<A>> threadLocalAccumulators;

    /**
     * Function used to create the R type based on the index of the corresponding T
     * type
     */
    public final Function<Long, A> initFunc;

    /**
     * Protected constructor. Initializes members common to all
     * ThreadLocalAccumulator implementations
     *
     * @param initializerFunction function used to initialize a new accumulator A
     *                            for the T object located at the specified index
     */
    protected Accumulator(Function<Long, A> initializerFunction) {
        initFunc = initializerFunction;
        threadLocalAccumulators = new ArrayList<>();
    }

    /**
     * Obtain all currently initialized thread-local accumulators.
     *
     * @return all thread accumulators initialized by this instance up until this
     *         point
     */
    public List<ThreadLocalAccumulator<A>> getAllThreadLocalAccumulator() {
        return threadLocalAccumulators;
    }

    /**
     * Factory method which creates a new ThreadLocalAccumulator for an upcoming
     * thread to use to accumulate information.
     * <p>
     * This method is for internal use only. It is used when there are not enough
     * thread-local accumulators for the level of parallelism desired. Each
     * {@link Accumulator} implementation should implement this method to return a
     * {@link ThreadLocalAccumulator} instance fitted with its appropriate
     * accumulator allocation policy.
     *
     * @return a newly created
     */
    protected abstract ThreadLocalAccumulator<A> newThreadLocalAccumulator();

    /**
     * Obtain the specified number of thread-local accumulators. These can then be
     * used by as many threads to accumulate information without interfering with
     * each-other.
     * <p>
     * Previously allocate thread-local accumulators will be re-used by this class.
     * Only in a case where more accumulators then previously initialized are
     * demanded will new TLAs be allocated.
     *
     * @param nbAccumulators number of accumulator desired
     * @return list of accumulators containing as many
     */
    public List<ThreadLocalAccumulator<A>> obtainThreadLocalAccumulators(int nbAccumulators) {
        // Until we have enough local accumulators prepared, allocate some more
        while (threadLocalAccumulators.size() < nbAccumulators) {
            threadLocalAccumulators.add(newThreadLocalAccumulator());
        }
        return threadLocalAccumulators.subList(0, nbAccumulators);
    }

    /**
     * Discards all initialized accumulators prepared up until this point. Call this
     * method if you plan to re-use this {@link Accumulator} again for a new
     * computation.
     */
    public void reset() {
        threadLocalAccumulators.clear();
    }
}