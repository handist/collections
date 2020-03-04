package handist.util.dist;

import java.io.IOException;
import java.io.ObjectOutputStream;

public interface Serializer {
	void accept(ObjectOutputStream out) throws IOException;
}
