package handist.collections;

import handist.collections.dist.util.Pair;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.function.Consumer;

public class RangedListProduct<S,T> implements SquareRangedList<Pair<S,T>> {
    private final RangedList<S> first;
    private final RangedList<T> second;

    RangedListProduct(RangedList<S> first, RangedList<T> second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public RangedList<Pair<S, T>> getRowView(long row) {
        final S s0 = first.get(row);
        return new LazyRangedList<>(second, (T t0)-> new Pair<>(s0, t0));
    }

    @Override
    public RangedList<Pair<S, T>> getColumnView(long column) {
        final T t0 = second.get(column);
        return new LazyRangedList<>(first, (S s0)-> new Pair<>(s0, t0));
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        first.forEach((long column, S s0)->{
            columnAction.accept(column, new LazyRangedList<>(second, (T t0)-> new Pair<>(s0, t0)));
        });
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        second.forEach((long row, T t0)->{
            rowAction.accept(row, new LazyRangedList<>(first, (S s0)-> new Pair<>(s0, t0)));
        });
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asRowList() {
        return new LazyRangedList<>(first, (S s0)-> {
            return new LazyRangedList<>(second, (T t0) -> {
                return new Pair(s0, t0);
            });
        });
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asColumnList() {
        return new LazyRangedList<>(second, (T t0) -> {
            return new LazyRangedList<>(first, (S s0)-> {
                return new Pair(s0, t0);
            });
        });
    }

    @Override
    public void forEach(SquareRange range, Consumer<SquareSiblingAccessor<Pair<S, T>>> action) {

    }

    @Override
    public void forEach(SquareIndexTConsumer<? super Pair<S, T>> action) {
        first.forEach((long row, S s)-> {
            second.forEach((long column, T t)->{
                action.accept(row, column, new Pair<>(s, t));
            });
        });
    }

    @Override
    public void forEach(Consumer<? super Pair<S, T>> action) {
        for(S s: first) {
            for(T t: second) {
                action.accept(new Pair<>(s,t));
            }
        }
    }

    @Override
    public Pair<S, T> get(long index, long index2) {
        return new Pair<>(first.get(index), second.get(index));
    }

    @Override
    public SquareRange getRange() {
        return new SquareRange(first.getRange(), second.getRange());
    }

    @Override
    public Pair<S, T> set(long index, long index2, Pair<S, T> value) {
        throw new UnsupportedOperationException("set is not supported by RangedListProduct.");
    }
}


