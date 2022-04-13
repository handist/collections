package handist.collections;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import handist.collections.accumulator.Accumulator;
import handist.collections.accumulator.Accumulator.ThreadLocalAccumulator;
import handist.collections.dist.TeamedPlaceGroup;
import handist.collections.dist.util.Pair;
import handist.collections.function.LongTBiConsumer;
import handist.collections.function.MultiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.function.TriConsumer;
import handist.collections.util.Splitter;

class SimpleRangedListProduct<S, T> extends RangedListProduct<S, T> {

    private final RangedList<S> first;
    private final RangedList<T> second;
    private final SquareRange range;

    private final boolean simpleRect;

    SimpleRangedListProduct(RangedList<S> first, RangedList<T> second) {
        this.first = first;
        this.second = second;
        this.range = new SquareRange(first.getRange(), second.getRange());
        this.simpleRect = true;
    }

    SimpleRangedListProduct(RangedList<S> first, RangedList<T> second, boolean isUpperRect) {
        this(first, second, new SquareRange(first.getRange(), second.getRange(), isUpperRect));
    }

    SimpleRangedListProduct(RangedList<S> first, RangedList<T> second, SquareRange range) {
        this.first = first;
        this.second = second;
        this.range = range;
        this.simpleRect = !range.isUpperTriangle;
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asColumnList() {
        return new LazyRangedList<>(second, (long column, T t0) -> {
            return new LazyRangedList<>(getFirstView(column), (long row, S s0) -> {
                return new Pair<>(s0, t0);
            });
        });
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asRowList() {
        return new LazyRangedList<>(first, (long row, S s0) -> {
            return new LazyRangedList<>(getSecondView(row), (long column, T t0) -> {
                return new Pair<>(s0, t0);
            });
        });
    }

    @Override
    public void forEach(Consumer<? super Pair<S, T>> action) {
        first.forEach((long row, S s) -> {
            getSecondView(row).forEach((T t) -> {
                action.accept(new Pair<>(s, t));
            });
        });
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super Pair<S, T>> action) {
        first.forEach((long row, S s) -> {
            getSecondView(row).forEach((long column, T t) -> { // TK row dependent filter
                action.accept(row, column, new Pair<>(s, t));
            });
        });
    }

    @Override
    public void forEach(SquareRange range, Consumer<? super Pair<S, T>> action) {
        subView(range).forEach(action);
    }

    @Override
    public void forEach(SquareRange range, SquareIndexTConsumer<? super Pair<S, T>> action) {
        subView(range).forEach(action);
    }

    @Override
    public void forEachColumn(LongRange cRange, LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        second.forEach(cRange, (long column, T t0) -> {
            columnAction.accept(column,
                    new LazyRangedList<>(getFirstView(column), (long row, S s0) -> new Pair<>(s0, t0)));
        });
    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        second.forEach((long column, T t0) -> {
            columnAction.accept(column,
                    new LazyRangedList<>(getFirstView(column), (long row, S s0) -> new Pair<>(s0, t0)));
        });
    }

    @Override
    public void forEachRow(BiConsumer<S, RangedList<T>> consumer) {
        first.forEach((long row, S s) -> {
            consumer.accept(s, getSecondView(row));
        });
    }

    @Override
    public void forEachRow(LongRange rRange, LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        first.forEach(rRange, (long row, S s0) -> {
            rowAction.accept(row, new LazyRangedList<>(getSecondView(row), (long column, T t0) -> new Pair<>(s0, t0)));
        });
    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        first.forEach((long row, S s0) -> {
            rowAction.accept(row, new LazyRangedList<>(getSecondView(row), (long column, T t0) -> new Pair<>(s0, t0)));
        });
    }

    @Override
    public void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<Pair<S, T>>> action) {
        throw new UnsupportedOperationException("not implemented yet");
    }

    @Override
    public Pair<S, T> get(long index, long index2) {
        // range check dependent
        return new Pair<>(first.get(index), second.get(index));
    }

    @Override
    public RangedList<Pair<S, T>> getColumnView(long column) {
        final T t0 = second.get(column);
        return new LazyRangedList<>(getFirstView(column), (long row, S s0) -> new Pair<>(s0, t0));
    }

    private RangedList<S> getFirstView(long column) {
        return simpleRect ? first : first.subList(range.columnRange(column));
    }

    @Override
    public SquareRange getRange() {
        return range;
    }

    @Override
    public RangedList<Pair<S, T>> getRowView(long row) {
        final S s0 = first.get(row);
        return new LazyRangedList<>(getSecondView(row), (long column, T t0) -> new Pair<>(s0, t0));
    }

    private RangedList<T> getSecondView(long row) {
        return simpleRect ? second : second.subList(range.columnRange(row));
    }

    @Override
    public Iterator<Pair<S, T>> iterator() {
        return new SquareChunkIterator<>(range, toArray());
    }

    @Override
    public <A> void parallelForEach(Accumulator<A> acc, int parallelism,
            BiConsumer<Pair<S, T>, ThreadLocalAccumulator<A>> action) {
        // TODO
    }

    @Override
    public <A, O> void parallelForEachRow(Accumulator<A> acc, int parallelism,
            MultiConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier) {
        // TODO Auto-generated method stub
    }

    @Override
    public <A> void parallelForEachRow(Accumulator<A> acc, int parallelism,
            TriConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>> action) {
        // TODO Auto-generated method stub
    }

    /**
     * Set is not supported by RangedListProduct.
     */
    @Deprecated
    @Override
    public Pair<S, T> set(long index, long index2, Pair<S, T> value) {
        throw new UnsupportedOperationException("set is not supported by RangedListProduct.");
    }

    @Override
    public Iterator<Pair<S, T>> subIterator(SquareRange range) {
        final SquareRange r = range.intersection(getRange());
        return new SquareChunkIterator<>(r, getRange(), toArray());
    }

    @Override
    public SimpleRangedListProduct<S, T> subView(SquareRange range) {
        range = getRange().intersection(range);
        return new SimpleRangedListProduct<>(first.subList(range.outer), second.subList(range.inner), range);
    }

    @Override
    public RangedListProduct<S, T> teamedSplit(int outer, int inner, TeamedPlaceGroup pg, long seed) {
        final int numHosts = pg.size();
        final int ithHost = pg.rank();
        final Random rand = new Random(seed);
        final List<SquareRange> ranges = splitRange(outer, inner);
        if (rand != null) {
            Collections.shuffle(ranges, rand);
        }

        final Splitter split = new Splitter(ranges.size(), numHosts);
        final List<SquareRange> filteredRanges = split.getIth(ithHost, ranges);

        return new FilteredRangedListProduct<>(this, filteredRanges);
    }

    @Override
    public Object[] toArray() {
        final Object[] o = new Object[(int) range.size()];
        final int count[] = { 0 };
        forEach((Pair<S, T> pair) -> {
            o[count[0]] = pair;
            count[0]++;
        });
        return o;
    }

    @Override
    public Object[] toArray(SquareRange newRange) {
        final Object[] o = new Object[(int) newRange.size()];
        final int count[] = { 0 };
        forEach(newRange, (Pair<S, T> pair) -> {
            o[count[0]] = pair;
            count[0]++;
        });
        return o;
    }

    @Override
    public SquareChunk<Pair<S, T>> toChunk(SquareRange newRange) {
        final Object[] newRail = toArray(newRange); // check range at toArray
        if (newRail.length == 0) {
            throw new IllegalArgumentException("[RangedListProduct] toChunk(emptyRange) is not permitted.");
        }
        return new SquareChunk<>(newRange, newRail);
    }

    @Override
    public List<Pair<S, T>> toList(SquareRange newRange) {
        final ArrayList<Pair<S, T>> list = new ArrayList<>((int) newRange.size());
        forEach(newRange, (t) -> list.add(t));
        return list;
    }
}
