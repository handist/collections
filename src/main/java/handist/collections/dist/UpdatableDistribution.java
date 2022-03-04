package handist.collections.dist;

import apgas.Place;

/**
 * Abstract updateable distribution. This class can be subscribed to a
 * distributed collection to be automatically informed of changes in its
 * distribution through the package-visible methods. Subscribing an instance of
 * this class is done through method
 * {@link ElementLocationManageable#registerDistribution(UpdatableDistribution)}
 *
 * @author Patrick Finnerty
 *
 * @param <K> type used to identify entries in the distributed collection
 */
public abstract class UpdatableDistribution<K> {
    /**
     * Method used to update a distribution when an entry is removed from the
     * collection
     *
     * @param k the object identifying the entry which has been removed
     */
    abstract void removeLocation(K k);

    /**
     * Method used to update a distribution when an entry is either first added or
     * relocated to another location
     *
     * @param k        object used to identify the entry which has been added or
     *                 relocated
     * @param location the new location of said entry
     */
    abstract void updateLocation(K k, Place location);
}