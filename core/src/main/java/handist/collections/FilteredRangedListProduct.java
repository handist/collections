package handist.collections;

import static apgas.Constructs.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

class FilteredRangedListProduct<S, T> extends RangedListProduct<S, T> {

    /**
     * The ranges that this instance is in charge of
     */
    ArrayList<SquareRange> ranges;

    /**
     * The parent from which the ranges were filtered
     */
    SimpleRangedListProduct<S, T> parent;

    /**
     * Creates a product whose ranges it covers were filtered. The actual sub-ranges
     * contained by this instance are specified at the time of construction by
     * specifying in a list the actual square ranges that this object contains
     *
     * @param rangeList list of ranges actually covered by this object
     */
    public FilteredRangedListProduct(SimpleRangedListProduct<S, T> p, List<SquareRange> rangeList) {
        parent = p;
        ranges = new ArrayList<>();
        ranges.addAll(rangeList);
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asColumnList() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RangedList<RangedList<Pair<S, T>>> asRowList() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void forEach(Consumer<? super Pair<S, T>> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public void forEach(SquareIndexTConsumer<? super Pair<S, T>> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public void forEach(SquareRange subrange, Consumer<? super Pair<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.forEach(myRange.intersection(subrange), action);
        }

    }

    @Override
    public void forEach(SquareRange subrange, SquareIndexTConsumer<? super Pair<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.forEach(myRange.intersection(subrange), action);
        }
    }

    @Override
    public void forEachColumn(LongRange range, LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        // TODO Auto-generated method stub

    }

    @Override
    public void forEachColumn(LongTBiConsumer<RangedList<Pair<S, T>>> columnAction) {
        // TODO Auto-generated method stub

    }

    @Override
    public void forEachRow(BiConsumer<S, RangedList<T>> action) {
        // TODO Auto-generated method stub
    }

    @Override
    public void forEachRow(LongRange range, LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        // TODO Auto-generated method stub

    }

    @Override
    public void forEachRow(LongTBiConsumer<RangedList<Pair<S, T>>> rowAction) {
        // TODO Auto-generated method stub

    }

    @Override
    public void forEachWithSiblings(SquareRange range, Consumer<SquareSiblingAccessor<Pair<S, T>>> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public Pair<S, T> get(long index, long index2) {
        for (final SquareRange sr : ranges) {
            if (sr.contains(index, index2)) {
                return parent.get(index, index2);
            }
        }
        // In case out of range, what should be done?
        return null;
    }

    @Override
    public RangedList<Pair<S, T>> getColumnView(long column) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SquareRange getRange() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RangedList<Pair<S, T>> getRowView(long row) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterator<Pair<S, T>> iterator() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <A> void parallelForEach(Accumulator<A> acc, int parallelism,
            BiConsumer<Pair<S, T>, ThreadLocalAccumulator<A>> action) {
        // Prepare accumulators
        final List<ThreadLocalAccumulator<A>> tlas = acc.obtainThreadLocalAccumulators(parallelism);
        // Prepare the ranges processed by each thread
        final Splitter split = new Splitter(ranges.size(), parallelism);

        finish(() -> {
            for (int t = 0; t < parallelism; t++) {
                final int tf = t;
                async(() -> {
                    final List<SquareRange> assignedToThread = split.getIth(tf, ranges);
                    final ThreadLocalAccumulator<A> tla = tlas.get(tf);
                    final Consumer<? super Pair<S, T>> convertedAction = p -> action.accept(p, tla);
                    for (final SquareRange sr : assignedToThread) {
                        parent.subView(sr).forEach(convertedAction);
                    }
                });
            }
        });
    }

    @Override
    public <A, O> void parallelForEachRow(Accumulator<A> acc, int parallelism,
            MultiConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier) {
        // Prepare accumulators
        final List<ThreadLocalAccumulator<A>> tlas = acc.obtainThreadLocalAccumulators(parallelism);
        // Prepare the ranges processed by each thread
        final Splitter split = new Splitter(ranges.size(), parallelism);
        finish(() -> {
            for (int t = 0; t < parallelism; t++) {
                final int tf = t;
                async(() -> {
                    final List<SquareRange> assignedToThread = split.getIth(tf, ranges);
                    final List<RangedListProduct<S, T>> prodsAssignedToThread = parent.getViews(assignedToThread);
                    final ThreadLocalAccumulator<A> tla = tlas.get(tf);
                    final O o = oSupplier.apply(tf); // Obtain the O object for this thread
                    final BiConsumer<S, RangedList<T>> convertedAction = (s, p) -> action.accept(s, p, tla, o);
                    for (final RangedListProduct<S, T> prod : prodsAssignedToThread) {
                        prod.forEachRow(convertedAction);
                    }
                });
            }
        });

    }

    @Override
    public <A> void parallelForEachRow(Accumulator<A> acc, int parallelism,
            TriConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>> action) {
        // Prepare accumulators
        final List<ThreadLocalAccumulator<A>> tlas = acc.obtainThreadLocalAccumulators(parallelism);
        // Prepare the ranges processed by each thread
        final Splitter split = new Splitter(ranges.size(), parallelism);

        finish(() -> {
            for (int t = 0; t < parallelism; t++) {
                final int tf = t;
                async(() -> {
                    final List<SquareRange> assignedToThread = split.getIth(tf, ranges);
                    final List<RangedListProduct<S, T>> prodsAssignedToThread = parent.getViews(assignedToThread);
                    final ThreadLocalAccumulator<A> tla = tlas.get(tf);
                    final BiConsumer<S, RangedList<T>> convertedAction = (s, p) -> action.accept(s, p, tla);
                    for (final RangedListProduct<S, T> prod : prodsAssignedToThread) {
                        prod.forEachRow(convertedAction);
                    }
                });
            }
        });
    }

    @Deprecated
    @Override
    public Pair<S, T> set(long index, long index2, Pair<S, T> value) {
        return parent.set(index, index2, value);
    }

    @Override
    public Iterator<Pair<S, T>> subIterator(SquareRange range) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RangedListProduct<S, T> subView(SquareRange range) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RangedListProduct<S, T> teamedSplit(int outer, int inner, TeamedPlaceGroup pg, long seed) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] toArray() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Object[] toArray(SquareRange newRange) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SquareChunk<Pair<S, T>> toChunk(SquareRange newRange) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public List<Pair<S, T>> toList(SquareRange newRange) {
        // TODO Auto-generated method stub
        return null;
    }
}
