package handist.collections.glb.lifeline;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Lifeline implementation which provide no lifeline between places. Using this
 * lifeline implementation effectively disables any inter-host load balancing
 * mechanism.
 *
 * @author Patrick Finnerty
 *
 */
public class NoLifeline extends Lifeline {

    /** Constructor */
    public NoLifeline(TeamedPlaceGroup pg) {
        super(pg);
    }

    /**
     * Returns an empty array (no lifelines to establish)
     */
    @Override
    public List<Place> lifeline(Place p) {
        return new ArrayList<>(0);
    }

    /**
     * Returns an empty array (there are no places which can establish a lifeline on
     * this place
     */
    @Override
    public List<Place> reverseLifeline(Place p) {
        return new ArrayList<>(0);
    }

}
