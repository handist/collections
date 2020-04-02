package handist.util;

import java.util.function.Consumer;

interface MultiReceiver<T> {
    Consumer<T> getReceiver();
}
