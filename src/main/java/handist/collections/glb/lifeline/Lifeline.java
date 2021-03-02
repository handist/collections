package handist.collections.glb.lifeline;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import handist.collections.dist.TeamedPlaceGroup;

/**
 * Description of a lifeline.
 * <p>
 * A lifeline is a pre-defined link from a place "A" to a place "B" by which B
 * is signaled that A requires some work and that B should give some to A when
 * able.
 *
 * @author Patrick Finnerty
 *
 */
public abstract class Lifeline {

    /**
     * List of places considered for the lifeline network. They are sorted according
     * to the natural ordering of their place's id ({@link Place#id}). As we are
     * dealing with place groups that may not contain contiguous sets of places,
     * sorting them during initialization makes later manipulation easier as the
     * indices in this {@link ArrayList} can be used as a substitute to the "random"
     * list of places being manipulated.
     */
    ArrayList<Place> sortedListOfPlaces;

    /**
     * Constructor specifying the set of places considered for the lifeline network.
     */
    public Lifeline(TeamedPlaceGroup pg) {
        sortedListOfPlaces = new ArrayList<>(pg.size());
        for (final Place p : pg.places()) {
            sortedListOfPlaces.add(p);
        }

        sortedListOfPlaces.sort((a, b) -> {
            return a.id - b.id;
        });
    }

    /**
     * Returns the list of places on which place "p" can establish lifelines within
     * the provided place group
     *
     * @param p place establishing lifelines
     * @return the list of places on which p can establish lifelines
     */
    public abstract List<Place> lifeline(Place p);

    /**
     * Returns the list of places that can establish lifelines on place "p".
     *
     * @param p the receiver of lifeline considered
     * @return the list of places which are susceptible to establish a lifeline on
     *         "p"
     */
    public abstract List<Place> reverseLifeline(Place p);
}
