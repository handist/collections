package handist.distcolls.dist;

import java.io.ObjectInputStream;
import java.io.Serializable;

public interface DeSerializer extends Serializable{
    void accept(ObjectInputStream in) throws Exception;
}
