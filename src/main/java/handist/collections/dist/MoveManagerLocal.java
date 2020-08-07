/*******************************************************************************
 * Copyright (c) 2020 Handy Tools for Distributed Computing (HanDist) project.
 *
 * This program and the accompanying materials are made available to you under 
 * the terms of the Eclipse Public License 1.0 which accompanies this 
 * distribution, and is available at https://www.eclipse.org/legal/epl-v10.html
 *
 * SPDX-License-Identifier: EPL-1.0
 *******************************************************************************/
package handist.collections.dist;

import static apgas.Constructs.*;

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
import handist.collections.function.DeSerializer;
import handist.collections.function.Serializer;

/**
 * This class is used for relocating elements of DistCollections.
 */
public final class MoveManagerLocal {
	private static final boolean DEBUG = false;
	final Map<Place, List<DeSerializer>> builders;
	// TODO TeamedPlaceGroup or PlaceGroup<PlaceInTeam>
	final TeamedPlaceGroup placeGroup;
	final Map<Place, List<Serializer>> serializeListMap;


	/**
	 * Construct a MoveManagerLocal with the given arguments.
	 *
	 * @param placeGroup the group hosts that will transfer objects between
	 * 	themselves using this instance.
	 */

	public MoveManagerLocal(TeamedPlaceGroup placeGroup) {
		this.placeGroup = placeGroup;
		serializeListMap = new HashMap<>(placeGroup.size());
		builders = new HashMap<>(placeGroup.size());
		for (Place place: placeGroup.places()) {
			serializeListMap.put(place, new ArrayList<>());
			builders.put(place, new ArrayList<>());
		}
	}

	public void clear() {
		for (List<Serializer> list: serializeListMap.values()) {
			list.clear();
		}
		for (List<DeSerializer> list: builders.values()) {
			list.clear();
		}
	}

	@SuppressWarnings("unchecked")
	public void executeDeserialization(byte[] buf, int[] rcvOffset, int[] rcvSize) throws Exception {
		int current = 0;
		for(Place p: placeGroup.places()) {
			int size = rcvSize[current];
			int offset = rcvOffset[current];
			current++;
			if(p.equals(here())) continue;

			ByteArrayInputStream in = new ByteArrayInputStream(buf, offset, size);
			ObjectInputStream ds = new ObjectInputStream(in);
			List<DeSerializer> deserializerList =
					(List<DeSerializer>)ds.readObject();
			for (DeSerializer deserialize: deserializerList) {
				deserialize.accept(ds);
			}
		}
	}

	@SuppressWarnings("unchecked")
	public void executeDeserialization(Map<Place, byte[]> map) throws Exception {
		for(Place p: placeGroup.places()) {
			if(p.equals(here())) continue;
			byte[] buf = map.get(p);
			ObjectInputStream ds = new ObjectInputStream(new ByteArrayInputStream(buf));
			List<Consumer<ObjectInputStream>> deserializerList =
					(List<Consumer<ObjectInputStream>>)ds.readObject();
			for (Consumer<ObjectInputStream> deserialize: deserializerList) {
				deserialize.accept(ds);
			}
		}
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

	public void executeSerialization(TeamedPlaceGroup placeGroup2, ByteArrayOutputStream out, int[] offsets,
			int[] sizes) throws IOException {
		for (int i = 0; i < placeGroup2.size(); i++) {
			Place place = placeGroup2.get(i);
			// TODO is this correct??
			if (place.equals(here()))
				continue;
			offsets[i] = out.size();
			// TODO should reopen ByteArray...
			if(DEBUG) System.out.println("execSeri: " + here() + "->" + place + ":start:" + out.size());
			ObjectOutputStream s = new ObjectOutputStream(out);
			s.writeObject(builders.get(place));
			for (Serializer serializer : serializeListMap.get(place)) {
				serializer.accept(s);
			}
			s.close();
			if(DEBUG) System.out.println("execSeri: " + here() + "->" + place + ":finish:" + out.size());
			sizes[i] = out.size() - offsets[i];
		}
	}

	public void request(Place pl, Serializer serializer,
			DeSerializer deserializer) {
		serializeListMap.get(pl).add(serializer);
		builders.get(pl).add(deserializer);
	}

	/**
	 * Request to reset the Serializer at the specified place.
	 *
	 * @param pl the target place.
	 */
	public void reset(Place pl) {
		serializeListMap.get(pl).add((ObjectOutputStream s) -> {
			try {
				s.reset();
			} catch (IOException e) {}
		});
	}






	/**
	 * Execute the all requests synchronously.
	 * @throws Exception if a runtime exception is thrown at any stage during 
	 *  the relocation
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


