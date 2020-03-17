package handist.util.dist;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import apgas.Constructs;
import apgas.Place;
import mpi.MPI;
import mpi.MPIException;

public class CollectiveRelocator {

    public static void all2allser(TeamedPlaceGroup placeGroup, MoveManagerLocal mm) throws Exception {
        int[] sendOffset = new int[placeGroup.size()];
        int[] sendSize = new int[placeGroup.size()];
        int[] rcvOffset = new int[placeGroup.size()];
        int[] rcvSize = new int[placeGroup.size()];
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mm.executeSerialization(placeGroup, out, sendOffset, sendSize);
        byte[] buf = executeRelocation(placeGroup, out.toByteArray(), sendOffset, sendSize, rcvOffset, rcvSize);
        mm.executeDeserialization(buf, rcvOffset, rcvSize);
        mm.clear();
    }

    /* TODO　int->long??
            * 本当は、、int, long 版なども欲しいところだったような
     */
    static byte[] executeRelocation(TeamedPlaceGroup placeGroup,
            byte[] byteArray, int[] sendOffset, int[] sendSize,
            int[] rcvOffset, int[] rcvSize) throws MPIException {
        placeGroup.comm.Alltoall(sendSize, 0, 1, MPI.INT, rcvSize, 0, 1, MPI.INT);
        {
            StringBuffer buf = new StringBuffer();
            buf.append(Constructs.here()+"::");
            for(int j=0;j<rcvSize.length;j++) {
                buf.append(":"+rcvSize[j]);
            }
            System.out.println(buf.toString());
        }

        int current = 0;
        for(int i = 0; i<rcvSize.length; i++) {
            rcvOffset[i] = current;
            current += rcvSize[i];
        }
        byte[] recvbuf = new byte[current];
        placeGroup.comm.Alltoallv(byteArray, 0, sendSize, sendOffset, MPI.BYTE, recvbuf, 0, rcvSize, rcvOffset, MPI.BYTE);
        return recvbuf;
    }


    public static void allgatherSer(TeamedPlaceGroup pg,
            Consumer<ObjectOutputStream> ser,
            BiConsumer<ObjectInputStream, Place> deser) throws MPIException {
        int numPlaces = pg.size();
        ByteArrayOutputStream out0 = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(out0);
            ser.accept(out);
            out.close();
        } catch(IOException exp) {
            throw new Error("This should not occur!.");
        }
        byte[] buf = out0.toByteArray();
        int size = buf.length;
        int[] tmpCounts = new int[1];
        tmpCounts[0] = size;
        int[] recvCounts = new int[numPlaces];
        int[] recvDispls = new int[numPlaces];
        pg.comm.Allgather(tmpCounts, 0, 1, MPI.INT, recvCounts, 0, 1, MPI.INT);

        int total = 0;
        for (int i=0; i<recvCounts.length; i++) {
            recvDispls[i]= total;
            total += recvCounts[i];
        }
        byte[] rbuf = new byte[total];
        pg.comm.Allgatherv(buf, 0, size, MPI.BYTE, rbuf, 0, recvCounts, recvDispls, MPI.BYTE);

        for (int i=0; i<recvCounts.length; i++) {
            if(Constructs.here().equals(pg.get(i))) continue;
            ByteArrayInputStream in0 =
                    new ByteArrayInputStream(rbuf, recvDispls[i], recvCounts[i]);
            try {
                ObjectInputStream in = new ObjectInputStream(in0);;
                deser.accept(in, pg.get(i));
            } catch (IOException e) {
                throw new RuntimeException("This should not occur.");
            }
        }
    }

    public static void gatherSer(TeamedPlaceGroup pg, Place root,
            Consumer<ObjectOutputStream> ser,
            BiConsumer<ObjectInputStream, Place> deser) throws MPIException {
        int numPlaces = pg.size();
        ByteArrayOutputStream out0 = new ByteArrayOutputStream();
        try {
            ObjectOutputStream out = new ObjectOutputStream(out0);
            ser.accept(out);
            out.close();
        } catch(IOException exp) {
            throw new Error("This should not occur!.");
        }
        byte[] buf = out0.toByteArray();
        int size = buf.length;
        int[] tmpCounts = new int[1];
        tmpCounts[0] = size;
        int[] recvCounts = new int[numPlaces];
        int[] recvDispls = new int[numPlaces];
        pg.comm.Gather(tmpCounts, 0, 1, MPI.INT, recvCounts, 0, 1, MPI.INT, pg.rank(root));

        int total = 0;
        for (int i=0; i<recvCounts.length; i++) {
            recvDispls[i]= total;
            total += recvCounts[i];
        }
        byte[] rbuf = Constructs.here().equals(root)? new byte[total]: null;
        pg.comm.Gatherv(buf, 0, size, MPI.BYTE, rbuf, 0, recvCounts, recvDispls, MPI.BYTE, pg.rank(root));

        if(!Constructs.here().equals(root)) return;
        for (int i=0; i<recvCounts.length; i++) {
            if(Constructs.here().equals(pg.get(i))) continue;
            ByteArrayInputStream in0 =
                    new ByteArrayInputStream(rbuf, recvDispls[i], recvCounts[i]);
            try {
                ObjectInputStream in = new ObjectInputStream(in0);;
                deser.accept(in, pg.get(i));
            } catch (IOException e) {
                throw new RuntimeException("This should not occur.");
            }
        }
    }


    public static void bcastSer(TeamedPlaceGroup pg, Place root,
            Consumer<ObjectOutputStream> ser,
            Consumer<ObjectInputStream> des) throws MPIException {
        try {
            int[] tmpBuf = new int[1];
            if(Constructs.here().equals(root)) {
                ByteArrayOutputStream out0 = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(out0);
                ser.accept(out);
                out.close();
                tmpBuf[0] = out0.size();
                pg.comm.Bcast(tmpBuf, 0, 1, MPI.INT, pg.rank(root));
                pg.comm.Bcast(out0.toByteArray(), 0, out0.size(), MPI.BYTE, pg.rank(root));
            } else {
                pg.comm.Bcast(tmpBuf, 0, 1, MPI.INT, pg.rank(root));
                byte[] buf = new byte[tmpBuf[0]];
                pg.comm.Bcast(buf, 0, buf.length, MPI.BYTE, pg.rank(root));
                des.accept(new ObjectInputStream(new ByteArrayInputStream(buf)));
            }
        } catch (IOException e) {
            throw new Error("This should not occur!");
        }

    }
}

