package handist.collections.dist;

import java.io.ObjectInputStream;
import java.io.Serializable;

import apgas.Place;

public interface DeSerializerUsingPlace extends Serializable{
    void accept(ObjectInputStream in, Place p) throws Exception;
}
