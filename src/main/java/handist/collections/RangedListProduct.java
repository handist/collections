package handist.collections;

import handist.collections.dist.util.Pair;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.SquareIndexTConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class RangedListProduct<S, T> implements SquareRangedList<Pair<S, T>> {
    private final RangedList<S> first;
    private final RangedList<T> second;
    private final SquareRange range;
    private final boolean simpleRect;

    public RangedListProduct(RangedList<S> first, RangedList<T> second) {
        this.first = first;
        this.second = second;
        this.range = new SquareRange(first.getRange(), second.getRange());
        this.simpleRect = true;
    }
    public RangedListProduct(RangedList<S> first, RangedList<T> second, boolean isUpperRect) {
        this(first, second, new SquareRange(first.getRange(), second.getRange(), isUpperRect));
    }
    public RangedListProduct(RangedList<S> first, RangedList<T> second, SquareRange range) {
        this.first = first;
        this.second = second;
        this.range = range;
        this.simpleRect = !range.isUpperTriangle;
    }

    private RangedList<S> getFirstView(long column) {
        return simpleRect ? first : first.subList(range.columnRange(column));
    }
    private RangedList<T> getSecondView(long row) {
        return simpleRect ? second : second.subList(range.columnRange(row));
    }

    public void forEachRow(BiConsumer<S, RangedList<T>> consumer) {
        first.forEach((long row, S s)->{
            consumer.accept(s, getSecondView(row));
        });
    }

    @Override
    public RangedList<Pair<S, T>> getRowView(long row) {
        final S s0 = first.get(row);
        return new LazyRangedList<>(getSecondView(row), (long column,T t0) -> new Pair<>(s0, t0));
    }

    @Override
    public RangedList<Pair<S, T>> getColumnView(long column) {
        final T t0 = second.get(column);
        return new LazyRangedList<>(getFirstView(column), (long row, S s0) -> new Pair<>(s0, t0));
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        second.forEach((long column, T t0) -> {
            columnAction.accept(column, new LazyRangedList<>(getFirstView(column), (long row,S s0) -> new Pair<>(s0, t0)));
        });
    }
    @Override
    public void forEachColumn(LongRange cRange, LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        second.forEach(cRange, (long column, T t0) -> {
            columnAction.accept(column, new LazyRangedList<>(getFirstView(column), (long row,S s0) -> new Pair<>(s0, t0)));
        });
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        first.forEach((long row, S s0) -> {
            rowAction.accept(row, new LazyRangedList<>(getSecondView(row), (long column,T t0) -> new Pair<>(s0, t0)));
        });
    }
    @Override
    public void forEachRow(LongRange rRange, LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        first.forEach(rRange, (long row, S s0) -> {
            rowAction.accept(row, new LazyRangedList<>(getSecondView(row), (long column,T t0) -> new Pair<>(s0, t0)));
        });
    }


    @Override
    public RangedList<RangedList<Pair<S, T>>> asRowList() {
        return new LazyRangedList<>(first, (long row, S s0) -> {
            return new LazyRangedList<>(getSecondView(row), (long column, T t0) -> {
                return new Pair(s0, t0);
            });
        });
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asColumnList() {
        return new LazyRangedList<>(second, (long column, T t0) -> {
            return new LazyRangedList<>(getFirstView(column), (long row, S s0) -> {
                return new Pair(s0, t0);
            });
        });
    }

    @Override
    public void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<Pair<S, T>>> action) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super Pair<S, T>> action) {
        first.forEach((long row, S s) -> {
            getSecondView(row).forEach((long column, T t) -> {  // TK row dependent filter
                action.accept(row, column, new Pair<>(s, t));
            });
        });
    }
    @Override
    public void forEach(SquareRange range, SquareIndexTConsumer<? super Pair<S, T>> action) {
        subView(range).forEach(action);
    }

    @Override
    public void forEach(Consumer<? super Pair<S, T>> action) {
        first.forEach((long row, S s)->{
            getSecondView(row).forEach((T t)->{
                action.accept(new Pair<>(s, t));
            });
        });
    }
    @Override
    public void forEach(SquareRange range, Consumer<? super Pair<S, T>> action) {
        subView(range).forEach(action);
    }

    @Override
    public Pair<S, T> get(long index, long index2) {
        // range check dependent
        return new Pair<>(first.get(index), second.get(index));
    }

    @Override
    public SquareRange getRange() {
        return range;
    }

    @Override
    public Pair<S, T> set(long index, long index2, Pair<S, T> value) {
        throw new UnsupportedOperationException("set is not supported by RangedListProduct.");
    }

    @Override
    public RangedListProduct<S, T> subView(SquareRange range) {
        range = getRange().intersection(range);
        return new RangedListProduct<S, T>(first.subList(range.outer), second.subList(range.inner),range);
    }
    public List<RangedListProduct<S,T>> split2(int outer, int inner) {
        List<RangedListProduct<S,T>> results = new ArrayList<>();
        getRange().split(outer,inner).forEach((SquareRange range)->{
            results.add(subView(range));
        });
        return results;
    }
    public List<List<RangedListProduct<S,T>>> splitN2(int outer, int inner, int num, boolean randomize) {
        List<RangedListProduct<S,T>> flat = split2(outer,inner);
        if(randomize) Collections.shuffle(flat);
        List<List<RangedListProduct<S,T>>> results = new ArrayList<>();
        int div = flat.size() / num;
        int rem = flat.size() % num;
        int current = 0;
        for(int i=0; i<num; i++) {
            int next = current + div + (i<rem? 1:0);
            results.add(flat.subList(current, next));
            current = next;
        }
        return results;
    }
}
