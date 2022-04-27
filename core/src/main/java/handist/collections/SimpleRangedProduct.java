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
import handist.collections.function.MultiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.function.TriConsumer;
import handist.collections.reducer.Reducible;
import handist.collections.util.Splitter;

class SimpleRangedProduct<S, T> implements RangedProduct<S, T>, Reducible<Pair<S, T>> {

    private final RangedList<S> outer;
    private final RangedList<T> inner;
    private final SquareRange range;

    private final boolean simpleRect;

    SimpleRangedProduct(RangedList<S> outer, RangedList<T> inner) {
        this.outer = outer;
        this.inner = inner;
        this.range = new SquareRange(outer.getRange(), inner.getRange());
        this.simpleRect = true;
    }

    SimpleRangedProduct(RangedList<S> outer, RangedList<T> inner, boolean isUpperRect) {
        this(outer, inner, new SquareRange(outer.getRange(), inner.getRange(), isUpperRect));
    }

    SimpleRangedProduct(RangedList<S> outer, RangedList<T> inner, SquareRange range) {
        this.outer = outer;
        this.inner = inner;
        this.range = range;
        this.simpleRect = !range.isUpperTriangle;
    }

    @Override
    public void forEach(Consumer<? super Pair<S, T>> action) {
        outer.forEach((long i, S s) -> {
            getInnerView(i).forEach((T t) -> {
                action.accept(new Pair<>(s, t));
            });
        });
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super Pair<S, T>> action) {
        outer.forEach((long i, S s) -> {
            getInnerView(i).forEach((long j, T t) -> { // TK row dependent filter
                action.accept(i, j, new Pair<>(s, t));
            });
        });
    }

    @Override
    public void forEach(SquareRange subRange, Consumer<? super Pair<S, T>> action) {
        subView(subRange).forEach(action);
    }

    @Override
    public void forEach(SquareRange subRange, SquareIndexTConsumer<? super Pair<S, T>> action) {
        subView(subRange).forEach(action);

    }

    @Override
    public void forEachInner(BiConsumer<T, RangedList<S>> action) {
        inner.forEach((long i, T t) -> {
            action.accept(t, getOuterView(i));
        });
    }

    @Override
    public void forEachInner(LongRange range, BiConsumer<T, RangedList<S>> action) {
        inner.forEach(range, (long i, T t) -> {
            action.accept(t, getOuterView(i));
        });
    }

    @Override
    public void forEachOuter(BiConsumer<S, RangedList<T>> action) {
        outer.forEach((long i, S s) -> {
            action.accept(s, getInnerView(i));
        });
    }

    @Override
    public void forEachOuter(LongRange range, BiConsumer<S, RangedList<T>> action) {
        outer.forEach(range, (long i, S s) -> {
            action.accept(s, getInnerView(i));
        });
    }

    @Override
    public void forEachWithSiblings(SquareRange range,
            Consumer<SquareSiblingAccessor<Pair<? super S, ? super T>>> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public Pair<S, T> get(long index, long index2) {
        // range check dependent
        return new Pair<>(outer.get(index), inner.get(index));
    }

    @Override
    public RangedList<T> getInnerPairs(long outerIndex) {
        return new LazyRangedList<>(getInnerView(outerIndex), (long i, T t0) -> t0);
    }

    private RangedList<T> getInnerView(long index) {
        return simpleRect ? inner : inner.subList(range.columnRange(index));
    }

    @Override
    public RangedList<S> getOuterPairs(long innerIndex) {
        return new LazyRangedList<>(getOuterView(innerIndex), (long i, S s0) -> s0);
    }

    private RangedList<S> getOuterView(long index) {
        return simpleRect ? outer : outer.subList(range.columnRange(index));
    }

    @Override
    public SquareRange getRange() {
        return range;
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
    public <A, O> void parallelForEachOuter(Accumulator<A> acc, int parallelism,
            MultiConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier) {
        // TODO Auto-generated method stub
    }

    @Override
    public <A> void parallelForEachOuter(Accumulator<A> acc, int parallelism,
            TriConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>> action) {
        // TODO Auto-generated method stub
    }

    @Override
    public Iterator<Pair<S, T>> subIterator(SquareRange range) {
        final SquareRange r = range.intersection(getRange());
        return new SquareChunkIterator<>(r, getRange(), toArray());
    }

    @Override
    public SimpleRangedProduct<S, T> subView(SquareRange range) {
        range = getRange().intersection(range);
        return new SimpleRangedProduct<>(outer.subList(range.outer), inner.subList(range.inner), range);
    }

    @Override
    public RangedProductList<S, T> teamedSplit(int outer, int inner, TeamedPlaceGroup pg, long seed) {
        final int numHosts = pg.size();
        final int ithHost = pg.rank();
        final Random rand = new Random(seed);
        final List<SquareRange> ranges = splitRange(outer, inner);
        if (rand != null) {
            Collections.shuffle(ranges, rand);
        }

        final Splitter split = new Splitter(ranges.size(), numHosts);
        final List<SquareRange> filteredRanges = split.getIth(ithHost, ranges);

        return new RangedProductList<>(this, filteredRanges);
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
