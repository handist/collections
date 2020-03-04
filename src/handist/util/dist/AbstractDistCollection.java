package handist.util.dist;

import static apgas.Constructs.*;

import java.io.IOException;
import java.io.Serializable;

import apgas.util.GlobalID;

public abstract class AbstractDistCollection<T> implements Serializable {

	final TeamedPlaceGroup placeGroup;
	final GlobalID id;
    //@TransientInitExpr(getLocalData())
    protected transient T data;

    protected abstract T getInitData();
    public AbstractDistCollection(TeamedPlaceGroup pg, T data) {
    	id = new GlobalID();
    	this.placeGroup = pg;
    	id.putHere(data);
	}
// TODO make(pg, init) 系も欲しい

    private void readObject(java.io.ObjectInputStream in)
    	     throws IOException, ClassNotFoundException {
    	in.defaultReadObject();
    	this.data = (T)id.getHere();
    	if(data==null) {
    		this.data = (T)id.putHereIfAbsent(getInitData());
    	}
    }

    /**
     * Return the PlaceGroup.
     *
     * @return PlaceGroup.
     */
    public TeamedPlaceGroup placeGroup() { return placeGroup; }

    public T localData() { return data; }

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

    public String toString() {
        return "[DistCol@"+here()+", "+ localData()+"]";
    }
}
