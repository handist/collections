package handist.collections.dist;

import java.util.Collection;
import java.util.Map.Entry;

import apgas.Place;

public interface RangeCachable<R> {

    public default void shareRangeAtSync(final Collection<R> ranges, final CacheRangedDistribution<R> destination,
            final MoveManager mm) {
        for (final R range : ranges) {
            shareRangeAtSync(range, destination, mm);
        }
    }

    public default void shareRangeAtSync(final Collection<R> ranges, final Collection<Place> destinations,
            MoveManager mm) {
        for (final Place dest : destinations) {
            shareRangeAtSync(ranges, dest, mm);
        }
    }

    public void shareRangeAtSync(final Collection<R> ranges, final Place destination, MoveManager mm);

    public default void shareRangeAtSync(final R range, final CacheRangedDistribution<R> destination,
            final MoveManager mm) {
        for (final Entry<R, Collection<Place>> mappings : destination.rangeLocation(range).entrySet()) {
            for (final Place place : mappings.getValue()) {
                shareRangeAtSync(mappings.getKey(), place, mm);
            }
        }
    }

    public default void shareRangeAtSync(final R range, final Collection<Place> destinations, final MoveManager mm) {
        for (final Place dest : destinations) {
            shareRangeAtSync(range, dest, mm);
        }
    }

    public void shareRangeAtSync(final R range, final Place destination, final MoveManager mm);

}
