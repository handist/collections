package handist.collections.dist;

import java.io.ObjectStreamException;
import java.io.Serializable;

import apgas.Constructs;
import apgas.Place;
import apgas.util.SerializableWithReplace;
import handist.collections.dist.util.SerializableBiFunction;

/**
 * This class is used to make a distributed object that wraps a Distributed
 * Collection instance and holds some variables on places. When implementing
 * such a class called {@code DistSample}, please inherit this class and prepare
 * the method `getBranchCreator()` that defines how the class create a remote
 * branch (instance of the class) on remote places. Please see the
 * {@code DistLog} class as an sample.
 *
 *
 *
 * @param <B> the target DistributedCollection class to be wrapped. The instance
 *            of this class will be associated with the instance of B.
 * @param <W> the implementing class itself (used for methods that operate on
 *            the implementing type itself). The implementing class will be
 *            defined as
 *            {@code class DistSample extends DistCollectionSatellite<P, DistSample>}.
 */
public abstract class DistCollectionSatellite<B extends DistributedCollection<?, ? super B>, W extends DistCollectionSatellite<B, W>>
        implements SerializableWithReplace {

    public static class Handle<B extends DistributedCollection<?, ? super B>, W extends DistCollectionSatellite<B, W>>
            implements Serializable {
        /** Serial Version UID */
        private static final long serialVersionUID = 3644465618475412725L;
        B base;
        SerializableBiFunction<B, Place, W> init;

        public Handle(B base, SerializableBiFunction<B, Place, W> init) {
            this.base = base;
            this.init = init;
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        public Object readResolve() {
            DistCollectionSatellite result = base.getSatellite();
            if (result == null) {
                result = init.apply(base, Constructs.here());
                base.setSatellite(result);
            }
            return result;
        }
    }

    B base;

    public DistCollectionSatellite(B base) {
        this.base = base;
    }

    /**
     * Please define how the new branch of this class will be created on remote
     * places.
     *
     * @return A initializer of the new branch that receives the wrapped distributed
     *         collection instance and the place, and creates a new branch on the
     *         place.
     */
    abstract public SerializableBiFunction<B, Place, W> getBranchCreator();

    protected B getPlanet() {
        return base;
    }

    public Object writeReplace() throws ObjectStreamException {
        final B base0 = this.base;
        final SerializableBiFunction<B, Place, W> init = getBranchCreator();
        return new Handle<>(base0, init);
    }
}
