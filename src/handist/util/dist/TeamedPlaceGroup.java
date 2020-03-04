package handist.util.dist;

import static apgas.Constructs.*;

import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import apgas.SerializableJob;
import apgas.util.GlobalID;
import apgas.util.PlaceLocalObject;
import mpi.Intracomm;
import mpi.MPI;

// TODO split, merge with ResilientPlaceGroup, ..
public class TeamedPlaceGroup extends PlaceLocalObject {

	public static TeamedPlaceGroup world;
	protected final List<Place> places;
	protected final Place[] rank2place;
	protected int size;
	// TODO
	Intracomm comm = MPI.COMM_WORLD;

	protected TeamedPlaceGroup() {
		places = new ArrayList<>(places());
		size = places.size();
		rank2place = new Place[size];
	}
	protected TeamedPlaceGroup init() {
		//TODO
		// setup MPI
		// setup arrays
		// setup rank2place
		// share the infromation
		// set this to singleton
		return this;
	}

	public void worldSetup() {
		PlaceLocalObject.make(places(), ()->{
			return new TeamedPlaceGroup().init();
		});
	}

	List<Place> toList() {
		return places;
	}
	public int size() {
		return size;
	}
	public Place get(int rank) {
		return places.get(rank);
	}
	public Place rank(int i) {
		return rank2place[i];
	}
	public int rank(Place place) {
		int index=0;
		for(Place p: rank2place) {
			if(p.equals(place)) return index;
			index++;
		}
		return -1;
	}
	// TODO
	// split, relocate feature
	public void remove(GlobalID id) {
		// TODO 自動生成されたメソッド・スタブ

	}


	public void broadcastFlat(SerializableJob run) {
		// TODO
		finish(()-> {
			for(Place p: this.places) {
				asyncAt(p, ()->{
					run.run();
				});
			}
		});
	}

	void test() {
		this.broadcastFlat(()->{
			System.out.println("hello" + here());

		});
	}
}

