package handist.distcolls.function;

public interface LTConsumer<T> {
    void accept(long l, T t);

    default LTConsumer<T> andThen(LTConsumer<? super T> after) {
        return new LTConsumer<T>() {
            @Override
            public void accept(long l, T t) {
                this.accept(l, t);
                after.accept(l, t);
            }
        };
    }
}