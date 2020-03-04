package handist.util.dist;

import java.io.ObjectInputStream;

public interface DeSerializer {
	void accept(ObjectInputStream in) throws Exception;
}
