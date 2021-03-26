package handist.collections.dist;

import apgas.Place;
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

/**
 * MoveManager is an entity which handles the transfer or distributed
 * collections entries from one distributed collection to another. They are used
 * to guarantee that data transfers occur in a single "transaction". An
 * implementation of this interface needs to be given as parameter to the
 * methods defined in {@link KeyRelocatable} and {@link RangeRelocatable}.
 * <p>
 * {@link MoveManager} typically accumulate the serializers and deserializers
 * used to make the transfer until a trigger method specific to the
 * implementation is called. Only when this specific trigger is called that the
 * actual removal of instances from distributed collections and their
 * serialization will occur.
 *
 * @author Patrick Finnerty
 *
 */
public interface MoveManager {

    /**
     * Submits a pair of serializer and deserializer to the move manager along with
     * the place to which the entries handled by this pair of
     * serializer/deserializer should be transferred to
     *
     * @param dest        place to which objects should be transferred
     * @param serialize   serializer which will transform some objects into a byte
     *                    array
     * @param deserialize complement to the serializer which will transform a byte
     *                    array back into the original objects
     */
    void request(Place dest, Serializer serialize, DeSerializer deserialize);

}
