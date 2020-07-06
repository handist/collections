package handist.collections;

import java.util.function.Consumer;

public interface MultiReceiver<T> {
    Consumer<T> getReceiver();
}
