package handist.util.dist;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import apgas.Place;

/**
 * This class is used for relocating elements of DistCollections.
 */
public final class MoveManagerLocal {
	// TODO TeamedPlaceGroup or PlaceGroup<PlaceInTeam>
	final TeamedPlaceGroup placeGroup;
	final Map<Place, List<Serializer>> serializeListMap;
	final Map<Place, List<DeSerializer>> builders;


    /**
     * Construct a MoveManagerLocal with given arguments.
     *
     * @param placeGroup PlaceGroup.
     * @param team Team
     */

    public MoveManagerLocal(TeamedPlaceGroup placeGroup) {
    	this.placeGroup = placeGroup;
    	serializeListMap = new HashMap<>(placeGroup.size());
        builders = new HashMap<>(placeGroup.size());
        for (Place place: placeGroup.toList()) {
            serializeListMap.put(place, new ArrayList<>());
            builders.put(place, new ArrayList<>());
        }
    }

    public void request(Place pl, Serializer serializer,
    		DeSerializer deserializer) {
    	serializeListMap.get(pl).add(serializer);
    	builders.get(pl).add(deserializer);
    }

    public void clear() {
    	for (List<Serializer> list: serializeListMap.values()) {
            list.clear();
        }
        for (List<DeSerializer> list: builders.values()) {
            list.clear();
        }
    }

    /**
     * Request to reset the Serializer at the specified place.
     *
     * @param atPlace the target place.
     */
    public void reset(Place pl) {
        serializeListMap.get(pl).add((ObjectOutputStream s) -> {
        	try {
        		s.reset();
        	} catch (IOException e) {}
        });
    }

    public byte[] executeSerialization(Place place) throws Exception {
    	ByteArrayOutputStream out = new ByteArrayOutputStream();
    	ObjectOutputStream s = new ObjectOutputStream(out);
    	s.writeObject(builders.get(place));
    	for (Serializer serializer: serializeListMap.get(place)) {
    		serializer.accept(s);
    	}
    	s.close();
    	return out.toByteArray();
    }

    public void executeDeserialization(Map<Place, byte[]> map) throws Exception {
    	for(Place p: placeGroup.toList()) {
    		byte[] buf = map.get(p);
    		ObjectInputStream ds = new ObjectInputStream(new ByteArrayInputStream(buf));
            List<Consumer<ObjectInputStream>> deserializerList =
            		(List<Consumer<ObjectInputStream>>)ds.readObject();
            for (Consumer<ObjectInputStream> deserialize: deserializerList) {
            	deserialize.accept(ds);
            }
    	}
    }

	public void executeSerialization(TeamedPlaceGroup placeGroup2, ByteArrayOutputStream out, int[] offsets, int[] sizes) throws IOException {
		for(int i = 0; i<placeGroup2.size; i++) {
			Place place = placeGroup2.get(i);
			offsets[i]=out.size();
			// TODO should reopen ByteArray...
	    	ObjectOutputStream s = new ObjectOutputStream(out);
	    	s.writeObject(builders.get(place));
	    	for (Serializer serializer: serializeListMap.get(place)) {
	    		serializer.accept(s);
	    	}
	    	s.close();
	    	sizes[i] = out.size() - offsets[i];
		}
	}
    public void executeDeserialization(byte[] buf, int[] rcvOffset, int[] rcvSize) throws Exception {
    	int current = 0;
    	for(Place p: placeGroup.toList()) {
    		int size = rcvSize[current];
    		int offset = rcvOffset[current];
    		ByteArrayInputStream in = new ByteArrayInputStream(buf, offset, size);
    		ObjectInputStream ds = new ObjectInputStream(in);
            List<Consumer<ObjectInputStream>> deserializerList =
            		(List<Consumer<ObjectInputStream>>)ds.readObject();
            for (Consumer<ObjectInputStream> deserialize: deserializerList) {
            	deserialize.accept(ds);
            }
    	}
    }






    /**
     * Execute the all requests synchronously.
     */
    public void sync() throws Exception {
        CollectiveRelocator.all2allser(placeGroup, this);
    }


    /* 将来的に
      moveAtSync(dist:RangedDistribution, mm) を 持つものを interface 宣言するのかな？
      public def moveAssociativeCollectionsAtSync(dist: RangedDistribution, dists: List[RangedMoballe]) {

      }
    public def moveAssosicativeCollectionsAtSync(dist: Distribution[K]) {
        // add dist to the list to schedule
    }
    */
}


