package handist.util.dist;

import java.io.ObjectStreamException;
import java.io.Serializable;

import apgas.SerializableCallable;
import apgas.util.GlobalID;

public abstract class AbstractDistCollection implements Serializable {

    static class LazyObjectReference<T> implements Serializable {
        protected final TeamedPlaceGroup pg0;
        protected final GlobalID id0;
        protected SerializableCallable<T> init;

        protected LazyObjectReference(TeamedPlaceGroup pg, GlobalID id, SerializableCallable<T> init) {
            this.id0 = id;
            this.pg0 = pg;
            this.init = init;
        }

        private Object readResolve() throws ObjectStreamException {
            Object result = id0.getHere();
            if (result == null) {
                try {
                    T r = init.call();
                    id0.putHereIfAbsent(r);
                } catch (Exception e) {
                    throw new Error("[Abstract Dist Collection: init should not raise exceptions.");
                }
                return id0.getHere();
            } else {
                return result;
            }
        }

    }
    abstract public Object writeReplace() throws ObjectStreamException;
    //    return new LaObjectReference(id, ()->{ new AbstractDistCollection<>());


    public final TeamedPlaceGroup placeGroup; // may be packed into T? or globalID??
    final GlobalID id;
    //@TransientInitExpr(getLocalData())

    public AbstractDistCollection(TeamedPlaceGroup pg) {
        this(pg, new GlobalID());
    }
    protected AbstractDistCollection(TeamedPlaceGroup pg, GlobalID id) {
        this.id = id;
        this.placeGroup = pg;
        id.putHere(this);
    }

 // TODO make(pg, init) 系も欲しい
    /*
    private void readObject(java.io.ObjectInputStream in)
             throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.data = (T)id.getHere();
        if(data==null) {
            id.putHereIfAbsent(getInitData());
            this.data = (T)id.getHere();
        }
    }
    private void writeObject(java.io.ObjectOutputStream out)
            throws IOException {
        out.defaultWriteObject();
    }
*/
    /**
     * Return the PlaceGroup.
     *
     * @return PlaceGroup.
     */
    public TeamedPlaceGroup placeGroup() { return placeGroup; }
    public abstract void clear();

    //TODO
    //public abstract void integrate(T src);
    public abstract void balance();

    /**
     * Destroy an instance of AbstractDistCollection.
     */
    public void destroy() {
        placeGroup.remove(id);
    }

    /*
    public final def printAllData(){
        for(p in placeGroup){
            at(p){
                printLocalData();
            }
        }
    }*/

}
