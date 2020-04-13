package handist.distcolls.dist;

import static apgas.Constructs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import apgas.Place;
import apgas.SerializableJob;
import apgas.util.GlobalID;
import mpi.Datatype;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;

// TODO split, merge with ResilientPlaceGroup, ..
public class TeamedPlaceGroup implements Serializable {
    // TODO
    private static final class ObjectReference implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = -1948016251753684732L;
        private final GlobalID id;

        private ObjectReference(GlobalID id) {
            this.id = id;
        }

        private Object readResolve() throws ObjectStreamException {
            return id.getHere();
        }
    }

    final GlobalID id;
    List<Place> places;
    int[] place2rank;
    int size;
    int myrank;
    // TODO
    Intracomm comm = MPI.COMM_WORLD;

    static TeamedPlaceGroup world;
    public static TeamedPlaceGroup getWorld() { return world; }

    protected TeamedPlaceGroup(GlobalID id, int myrank, int size,  int[] rank2place) { // for whole_world
        this.id=id;
    this.size=size;
    this.myrank=myrank;
        this.places = new ArrayList<Place>(size);
        this.place2rank = new int[size];
        for(int i=0; i< rank2place.length; i++) {
            int p = rank2place[i];
            places.add(new Place(p));
            place2rank[p] = i;
        }
    id.putHere(this);
    }

    public Object writeReplace() throws ObjectStreamException {
        return new ObjectReference(id);
    }

    protected TeamedPlaceGroup init() {
        //TODO
        // setup MPI
      /*  if(!MPI.Initialized()) {
            throw new Error("[TeamedPlaceGroup] Please setup MPI first");
        }*/
        // setup arrays
        // setup rank2place
        // share the infromation
        // set this to singleton
        return this;
    }

    public static void worldSetup() throws Exception { // must be called at initialization of MPI process.
        int myrank = MPI.COMM_WORLD.Rank();
        int size = MPI.COMM_WORLD.Size();
        int[] rank2place = new int[size];
        Place here = here();
        System.out.println("world setup: rank=" + myrank + ", place" + here + "::"+ here.id);
        rank2place[myrank]= here.id;
        MPI.COMM_WORLD.Allgather(rank2place, myrank, 1, MPI.INT, rank2place, 0, 1, MPI.INT);
        for(int i = 0; i<rank2place.length;i++) {
            System.out.println("ws: " + i +":"+rank2place[i] +"@"+myrank);
        }
        GlobalID id;
        if(myrank==0) { // TODO or here()
            id = new GlobalID();
            try {
                ByteArrayOutputStream out0 = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(out0);
                out.writeObject(id);
                out.close();
                byte[] buf = out0.toByteArray();
                int[] buf0 = new int[1];
                buf0[0]=buf.length;

                MPI.COMM_WORLD.Bcast(buf0, 0, 1, MPI.INT, 0);
                MPI.COMM_WORLD.Bcast(buf, 0, buf0[0], MPI.BYTE, 0);
            } catch (IOException e) {
                throw new Error("[TeamedPlaceGroup] init error at master!");
            }
        } else {
            int[] buf0 = new int[1];
            MPI.COMM_WORLD.Bcast(buf0, 0, 1, MPI.INT, 0);
            byte[] buf = new byte[buf0[0]];
            MPI.COMM_WORLD.Bcast(buf, 0, buf0[0], MPI.BYTE, 0);
            try {
                ObjectInputStream in =
                        new ObjectInputStream(new ByteArrayInputStream(buf));
                id = (GlobalID)in.readObject();
            } catch (Exception e) {
                throw new Error("[TeamedPlaceGroup] init error at worker");
            }
        }
        world = new TeamedPlaceGroup(id, myrank, size, rank2place);

        /*
        PlaceLocalObject.make(places(), ()->{
            return new TeamedPlaceGroup().init();
        });
        */
    }

    List<Place> places() {
        return places;
    }
    public int size() {
        return size;
    }
    public Place get(int rank) {
        return places.get(rank);
    }
    public int rank(Place place) {
        return place2rank[place.id];
    }
    public String toString() {
    return "TeamedPlaceGroup[" + id + ", myrank" + myrank + ", places" + places();
    }

    // TODO
    // split, relocate feature
    public void remove(GlobalID id) {
        // TODO

    }


    public void broadcastFlat(SerializableJob run) {
        // TODO
        finish(()-> {
            for(Place p: this.places()) {
                if(!p.equals(here()))
                    asyncAt(p, run);
            }
            run.run();
        });
    }

    public void Alltoallv(Object byteArray, int soffset, int[] sendSize, int[] sendOffset, Datatype stype,
            Object recvbuf, int roffset, int[] rcvSize, int[] rcvOffset, Datatype rtype) throws MPIException {
        if(false) {
            this.comm.Alltoallv(byteArray, soffset, sendSize, sendOffset, stype, recvbuf,  roffset,  rcvSize,  rcvOffset, rtype);
        } else {
            for(int rank=0; rank<rcvSize.length; rank++) {
                this.comm.Gatherv(byteArray, soffset + sendOffset[rank], sendSize[rank], stype,
                        recvbuf, roffset, rcvSize, rcvOffset, rtype, rank);
            }
        }
    }
    public void barrier()  {
        try {
            this.comm.Barrier();
        } catch (MPIException e) {
            e.printStackTrace();
            throw new Error("[TeamedPlaceGroup] MPI Exception raised.");
        }
    }

    public static void main(String[] args) {
        TeamedPlaceGroup t = getWorld();
        finish(() -> {
            t.broadcastFlat(() -> {
                System.out.println("hello:" + here() + ", " + t);
            });
        });
    }


}

