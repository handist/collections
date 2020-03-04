package handist.util.dist;


import static apgas.Constructs.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.BiFunction;

import apgas.Place;
import mpi.MPI;


// for internal use
// this is a class for the load balancing
abstract class LoadBalancer {

    //private static Place tmpRoot = Place(0);


    // private List<T> list;
	abstract int localSize();
	abstract void exportOne(ObjectOutputStream out) throws IOException;
	abstract void importOne(ObjectInputStream in) throws ClassNotFoundException, IOException;

    private TeamedPlaceGroup pg;
    private Place root;
    private int myRole;
    private ArrayList<Integer> senders;
    private ArrayList<Integer> receivers;

    public LoadBalancer(/*List<T> list,*/ TeamedPlaceGroup pg) {
        //this.list = list;
        this.pg = pg;
        this.root = pg.get(0);
        this.myRole = pg.rank(here());
        this.senders = new ArrayList<>(pg.size());
        this.receivers = new ArrayList<>(pg.size());
    }

    public void execute() {
        if (pg.size() == 1) return;
        relocate(getMoveCount());
    }


    // return (fromId, toId) => moveCount
    private BiFunction<Integer,Integer,Integer> getMoveCount() {
    	final int np = pg.size();
        int[] matrix = new int[np * np];
        this.senders.clear();
        this.receivers.clear();
        long[] tmpOverCounts = new long[np];
        Arrays.fill(tmpOverCounts, localSize());
        long[] overCounts = new long[np];
        //team.alltoall(tmpOverCounts, 0, overCounts, 0, 1);
        pg.comm.Alltoall(tmpOverCounts, 0, 1, MPI.LONG, overCounts, 0, 1, MPI.LONG);
        long total = 0;
        for (int i=0; i<np; i++) {
            total = total + overCounts[i];
        }
        long average = total / np;
        for (int i=0; i<np; i++) {
            overCounts[i] = overCounts[i] - average;
        }
        for (int i=0; i<np; i++) {
        	long overCount = overCounts[i];
            if (overCount < 0) {
                this.receivers.add(i);
            } else if (overCount > 0) {
                this.senders.add(i);
            }
        }
        if ((here().equals(root)) &&
        		(0 < this.senders.size()) && (0 < this.receivers.size())) {
        	Integer[] sendersX = new Integer[this.senders.size()];
        	Integer[] receiversX = new Integer[this.receivers.size()];
        	this.senders.toArray(sendersX);
        	Random random = new Random();
            for (int i=0; i<sendersX.length; i++) {
                Integer j = random.nextInt(sendersX.length);
                Integer tmp = sendersX[j];
                sendersX[j] = sendersX[i];
                sendersX[i] = tmp;
            }
            this.receivers.toArray(receiversX);
            Comparator<Integer> comp = (Integer a, Integer b) -> {
            	return Long.compare(overCounts[b], overCounts[a]);
            };
            Arrays.sort(receiversX, comp);
            int senderPointer = 0;
            int receiverPointer = 1;
            while ((receiverPointer < receiversX.length)
            		&& (senderPointer < sendersX.length)) {
                int i = sendersX[senderPointer];
                int j = receiversX[receiverPointer - 1];
                int k = receiversX[receiverPointer];
                while ((overCounts[k] < overCounts[j]) && (0 < overCounts[i])) {
                    overCounts[i]--;
                    overCounts[k]++;
                    matrix[np * i + k]++;
                }
                if (overCounts[j] == overCounts[k]) {
                    receiverPointer++;
                }
                if (overCounts[i] == 0) {
                    senderPointer++;
                }
            }
            while (senderPointer < sendersX.length) {
                int i = sendersX[senderPointer];
                while (0 < overCounts[i]) {
                    receiverPointer = (receiverPointer + 1) % receiversX.length;
                    int j = receiversX[receiverPointer];
                    overCounts[i]--;
                    overCounts[j]++;
                    matrix[np * i + j]++;
                }
                senderPointer++;
            }
        }
        //team.bcast(tmpRoot, matrix, 0, matrix, 0, np * np);
        pg.comm.Bcast(matrix, 0, np*np, MPI.INT, pg.rank(root));
        BiFunction<Integer,Integer,Integer> func = (Integer i0, Integer j0) -> {
        	return matrix[np * i0 + j0];
        };
        return func;
    }

    // execute relocation using getCount function
    private void relocate(BiFunction<Integer,Integer,Integer> getCount) {
    	try {
    		int np = pg.size();
    		ByteArrayOutputStream s0 = new ByteArrayOutputStream();
    		int[] scounts = new int[np];
    		int[] sdispls = new int[np];
    		int[] rcounts = new int[np];
    		int s0used = 0;
    		for(int j=0; j<np; j++) {
    			int count = getCount.apply(myRole, j);
    			if(count > 0) {
    				ObjectOutputStream s = new ObjectOutputStream(s0);
    				s.writeInt(count);
    				for (int k=0; k<count; k++) {
    					exportOne(s);
    				}
    				s.flush();
    				int prev = s0used;
    				sdispls[j] = prev;
    				s0used = s0.size();
    				scounts[j] = s0used - prev;
    			} else {
    				sdispls[j] = s0used;
    				scounts[j] = 0;
    			}
    		}

    		pg.comm.Alltoall(scounts, 0, 1, MPI.INT, rcounts, 0, 1, MPI.INT);
    		byte[] sendbuf = s0.toByteArray();

    		int[] rdispls = new int[np];
    		int rused = 0;
    		for(int i=0; i<np; i++) {
    			rdispls[i] = rused;
    			rused += rcounts[i];
    		}
    		byte[] recvbuf = new byte[rused];
    		pg.comm.Alltoallv(sendbuf, 0, scounts, sdispls, MPI.BYTE, recvbuf, 0, rcounts, rdispls, MPI.BYTE);

    		for (int i=0; i<np; i++) {
    			if(rcounts[i] == 0) continue;
    			ByteArrayInputStream in = new ByteArrayInputStream(recvbuf, rdispls[i], rcounts[i]);
    			ObjectInputStream ds = new ObjectInputStream(in);
    			int count = ds.readInt();
    			assert(getCount.apply(i, myRole) == count);
    			for(int k =0; k<count; k++) {
    				importOne(ds);
    			}
    		}
    	} catch(Exception e) {
    		e.printStackTrace(System.err);
    		throw new Error("Exception during LoadBalance Relocation.");
    	}
    }
    static final class ListBalancer<T> extends LoadBalancer {

		private List<T> body;

		public ListBalancer(List<T> body, TeamedPlaceGroup pg) {
			super(pg);
			this.body = body;
		}

		@Override
		int localSize() {
			return body.size();
		}

		@Override
		void exportOne(ObjectOutputStream out) throws IOException {
			T one = body.remove(body.size()-1);
			out.writeObject(one);
		}

		@Override
		void importOne(ObjectInputStream in) throws ClassNotFoundException, IOException {
			body.add((T)in.readObject());
		}

    }

    static final class MapBalancer<K,V> extends LoadBalancer {

		private Map<K, V> body;

		public MapBalancer(Map<K,V> body, TeamedPlaceGroup pg) {
			super(pg);
			this.body = body;
		}

		@Override
		int localSize() {
			return body.size();
		}

		@Override
		void exportOne(ObjectOutputStream out) throws IOException {
			assert(!body.isEmpty());
			K key = body.keySet().iterator().next();
			V v = body.remove(key);
			out.writeObject(key);
			out.writeObject(v);
		}

		@Override
		void importOne(ObjectInputStream obj) throws ClassNotFoundException, IOException {
			K key = (K) obj.readObject();
			V v = (V) obj.readObject();
			body.put(key, v);
		}

    }
}
    /*
// test
class Main {

    public static def main(args: Rail[String]): void {
        val o = new Main();
        o.run();
    }

    def run(): void {
        val pg = Place.places();
        val team = Team(pg);
        pg.broadcastFlat(() => {
            val executor = new Executor(pg, team);
            executor.start();
        });
    }

    static class Executor(pg: PlaceGroup, team: Team) {

        transient var map: x10.util.Map[Long, String];

        def start(): void {
            initialize(() => 1000000);
            balance();
        }

        def initialize(count: ()=>Long): void {
            val begin = System.nanoTime();
            if (map == null) {
                map = new x10.util.HashMap[Long, String]();
            }
            val num = count();
            for (var i: Long = 0; i < num; i++) {
                map(num * here.id + i) = i.toString();
            }
            val end = System.nanoTime();
            Console.OUT.println(here + " initialize " + ((end - begin) * 1e-6) + " ms");
        }

        def balance(): void {
            val begin = System.nanoTime();
            val al = new ArrayList[x10.util.Map.Entry[Long, String]](map.size());
            al.addAll(map.entries());
            map.clear();
            val balancer = new LoadBalancer[x10.util.Map.Entry[Long, String]](al, pg, team);
            balancer.execute();
            for (e in al) {
                map(e.getKey()) = e.getValue();
            }
            val end = System.nanoTime();
            Console.OUT.println(here + " balance " + ((end - begin) * 1e-6) + " ms");
        }
    }
}
*/