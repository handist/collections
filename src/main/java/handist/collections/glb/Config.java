package handist.collections.glb;

import handist.collections.glb.lifeline.Loop;

/**
 * Helper class for the settings that influence the behavior of the Global Load
 * Balancer. All the settings presented here can be set by passing -D arguments
 * to the java command. As an alternative, you could also set them with
 * {@link System#setProperty(String, String)}. In the later case, be aware that
 * parts of the program may have already performed initializations based on the
 * value present before you made the change. Also, make sure that you make your
 * changes consistent across all hosts participating in the computation.
 *
 * @author Patrick Finnerty
 *
 */
public class Config {

    /**
     * Handy enumarator used by the GLB routines to determine which mode should be
     * used to make lifeline answers
     */
    enum LifelineAnswerMode {
        KRYO, MPI
    }

    /**
     * Option used to activate traces in the GLB
     */
    public static final String ACTIVATE_TRACE = "glb.trace";

    /**
     * Option used to set the granularity, that is the number of elements workers
     * process before checking the runtime.
     */
    public static final String GRANULARITY = "glb.grain";

    /**
     * Default value for {@link #GRANULARITY} option.
     */
    public static final int GRANULARITY_DEFAULT = 100;

    /**
     * Option used to set the work-stealing network between the hosts involved in
     * the computation.
     */
    public static final String LIFELINE_STRATEGY = "glb.lifeline";

    /**
     * Default value for {@link #LIFELINE_STRATEGY}. Is class {@link Loop}.
     */
    public static final String LIFELINE_STRATEGY_DEFAULT = Loop.class.getCanonicalName();

    /**
     * Option used to set the maximum number of concurrent workers used by the load
     * balancer. By default, the value returned by
     * {@link Runtime#availableProcessors()} is used.
     */
    public static final String MAXIMUM_WORKER_COUNT = "glb.workers";
    /**
     * Determines the technique used to answer lifelines. Options are:
     * <ul>
     * <li>{@value #SERIALIZATION_KRYO} (default option) which uses normal lambda
     * serialization to transmit instances
     * <li>{@value #SERIALIZATION_MPI} which relies on MPI to transmit the byte
     * array into which the object instances have been serialized
     * </ul>
     */
    public static final String SERIALIZATION = "glb.serialization";
    /** Option for setting {@link #SERIALIZATION} */
    public static final String SERIALIZATION_MPI = "mpi";

    /** Option for setting {@link #SERIALIZATION} */
    public static final String SERIALIZATION_KRYO = "kryo";

    /**
     * Retrieves the granularity to use as defined either by the
     * {@link #GRANULARITY_DEFAULT} property (if set) or by the default value
     * {@value #GRANULARITY_DEFAULT}.
     *
     * @return the granularity to be used by the GLB as an integer
     */
    static int getGranularity() {
        if (System.getProperties().containsKey(GRANULARITY)) {
            return Integer.parseInt(System.getProperty(GRANULARITY));
        } else {
            return GRANULARITY_DEFAULT;
        }
    }

    /**
     * Indicates which type of lifeline answer should be made
     *
     * @return a value indicating which type of lifeline answer should be made
     */
    static LifelineAnswerMode getLifelineSerializationMode() {
        final String setting = System.getProperty(SERIALIZATION, SERIALIZATION_KRYO);
        if (SERIALIZATION_MPI.equals(setting)) {
            return LifelineAnswerMode.MPI;
        } else {
            return LifelineAnswerMode.KRYO;
        }
    }

    /**
     * Returns the number of concurrent workers with which the GLB is configured to
     * run. Checks if the {@value #MAXIMUM_WORKER_COUNT} property was set. If so,
     * parses this property. Otherwise, uses the value returned by
     * {@link Runtime#availableProcessors()}.
     *
     * @return the number of concurrent workers with which the GLB can run
     */
    static int getMaximumConcurrentWorkers() {
        if (System.getProperties().containsKey(MAXIMUM_WORKER_COUNT)) {
            return Integer.parseInt(System.getProperty(MAXIMUM_WORKER_COUNT));
        } else {
            return Runtime.getRuntime().availableProcessors();
        }
    }
}
