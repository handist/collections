package handist.collections;

import static apgas.Constructs.*;

import java.util.Iterator;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import handist.collections.accumulator.Accumulator;
import handist.collections.accumulator.Accumulator.ThreadLocalAccumulator;
import handist.collections.dist.util.Pair;
import handist.collections.function.MultiConsumer;
import handist.collections.function.SquareIndexTConsumer;
import handist.collections.function.TriConsumer;
import handist.collections.util.Splitter;

public class RangedProductList<S, T> implements RangedProduct<S, T> {

    private final RangedProduct<S, T> parent;
    private final List<SquareRange> ranges;

    RangedProductList(RangedProduct<S, T> parent, List<SquareRange> ranges) {
        this.parent = parent;
        this.ranges = ranges;
    }

    @Override
    public void forEach(Consumer<? super Pair<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.forEach(myRange, action);
        }
    }

    @Override
    public void forEach(SquareIndexTConsumer<? super Pair<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.forEach(myRange, action);
        }
    }

    @Override
    public void forEach(SquareRange subRange, Consumer<? super Pair<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.forEach(myRange.intersection(subRange), action);
        }
    }

    @Override
    public void forEach(SquareRange subRange, SquareIndexTConsumer<? super Pair<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.forEach(myRange.intersection(subRange), action);
        }
    }

    @Override
    public void forEachInner(BiConsumer<T, RangedList<S>> action) {
        for (final SquareRange myRange : ranges) {
            parent.subView(myRange).forEachInner(action);
        }
    }

    @Override
    public void forEachInner(LongRange range, BiConsumer<T, RangedList<S>> action) {
        for (final SquareRange myRange : ranges) {
            parent.subView(myRange.intersection(myRange)).forEachInner(action);
        }
    }

    @Override
    public void forEachOuter(BiConsumer<S, RangedList<T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.subView(myRange).forEachOuter(action);
        }
    }

    @Override
    public void forEachOuter(LongRange range, BiConsumer<S, RangedList<T>> action) {
        for (final SquareRange myRange : ranges) {
            parent.subView(myRange.intersection(myRange)).forEachOuter(action);
        }
    }

    public void forEachProd(Consumer<RangedProduct<S, T>> action) {
        for (final SquareRange myRange : ranges) {
            action.accept(parent.subView(myRange));
        }
    }

    @Override
    public void forEachWithSiblings(SquareRange range,
            Consumer<SquareSiblingAccessor<Pair<? super S, ? super T>>> action) {
        // TODO Auto-generated method stub

    }

    @Override
    public Pair<S, T> get(long outerIndex, long innerIndex) {
        for (final SquareRange sr : ranges) {
            if (sr.contains(outerIndex, innerIndex)) {
                return parent.get(outerIndex, innerIndex);
            }
        }
        // In case out of range, what should be done?
        return null;
    }

    @Override
    public RangedList<T> getInnerPairs(long outerIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RangedList<S> getOuterPairs(long innerIndex) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SquareRange getRange() {
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
                    final Consumer<Pair<S, T>> convertedAction = p -> action.accept(p, tla);
                    for (final SquareRange sr : assignedToThread) {
                        parent.subView(sr).forEach(convertedAction);
                    }
                });
            }
        });
    }

    @Override
    public <A, O> void parallelForEachOuter(Accumulator<A> acc, int parallelism,
            MultiConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier) {
        // TODO
    }

    @Override
    public <A> void parallelForEachOuter(Accumulator<A> acc, int parallelism,
            TriConsumer<S, RangedList<T>, ThreadLocalAccumulator<A>> action) {
        // TODO
    }

    public <A> void parallelForEachProd(Accumulator<A> acc, int parallelism,
            BiConsumer<RangedProduct<S, T>, ThreadLocalAccumulator<A>> action) {
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
                    final Consumer<RangedProduct<S, T>> convertedAction = p -> action.accept(p, tla);
                    for (final SquareRange sr : assignedToThread) {
                        convertedAction.accept(parent.subView(sr));
                    }
                });
            }
        });
    }

    public <A, O> void parallelForEachProd(Accumulator<A> acc, int parallelism,
            TriConsumer<RangedProduct<S, T>, ThreadLocalAccumulator<A>, O> action, Function<Integer, O> oSupplier) {
        // Prepare accumulators
        final List<ThreadLocalAccumulator<A>> tlas = acc.obtainThreadLocalAccumulators(parallelism);
        // Prepare the ranges processed by each thread
        final Splitter split = new Splitter(ranges.size(), parallelism);
        finish(() -> {
            for (int t = 0; t < parallelism; t++) {
                final int tf = t;
                async(() -> {
                    final List<SquareRange> assignedToThread = split.getIth(tf, ranges);
                    final List<RangedProduct<S, T>> prodsAssignedToThread = parent.getViews(assignedToThread);
                    final ThreadLocalAccumulator<A> tla = tlas.get(tf);
                    final O o = oSupplier.apply(tf); // Obtain the O object for this thread
                    final Consumer<RangedProduct<S, T>> convertedAction = (p) -> action.accept(p, tla, o);
                    for (final RangedProduct<S, T> prod : prodsAssignedToThread) {
                        convertedAction.accept(prod);
                    }
                });
            }
        });
    }

    public void parallelForEachProd(int parallelism, Consumer<RangedProduct<S, T>> action) {
        // Prepare the ranges processed by each thread
        final Splitter split = new Splitter(ranges.size(), parallelism);
        finish(() -> {
            for (int t = 0; t < parallelism; t++) {
                final int tf = t;
                async(() -> {
                    final List<SquareRange> assignedToThread = split.getIth(tf, ranges);
                    final List<RangedProduct<S, T>> prodsAssignedToThread = parent.getViews(assignedToThread);
                    for (final RangedProduct<S, T> prod : prodsAssignedToThread) {
                        action.accept(prod);
                    }
                });
            }
        });
    }

    public List<SquareRange> ranges() {
        return ranges;
    }

    @Override
    public Iterator<Pair<S, T>> subIterator(SquareRange range) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public RangedProduct<S, T> subView(SquareRange range) {
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
