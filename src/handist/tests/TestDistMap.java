package handist.tests;

import static apgas.Constructs.*;

import apgas.Place;
import handist.util.dist.DistMap;
import handist.util.dist.TeamedPlaceGroup;

public class TestDistMap {
	// TODO
	public static void main(String[] args) {
		TeamedPlaceGroup world = TeamedPlaceGroup.world;
		DistMap<String, Integer> dmap = new DistMap<String,Integer>(world);
		DistMap<String, String> dmap2 = DistMap.<String, String>make(world,
				(Place p, DistMap<String,String> body)->{
			body.put(here()+":A", "A");
			body.put(here()+":B", "B");
		});
	}
}
