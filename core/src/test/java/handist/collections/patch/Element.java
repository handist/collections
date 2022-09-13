package handist.collections.patch;

import java.io.Serializable;

public class Element implements Positionable<Position2D>, Serializable {

    Position2D position;
    /** default is 0 */
    int value;

    /** patch index to trace the element after migrating between patches */
    public Index2D belongPatch;

    public Element(double x, double y) {
        position = new Position2D(x, y);
        belongPatch = new Index2D(0, 0);
        value = 0;
    }

    public Element(double x, double y, Index2D belong) {
        position = new Position2D(x, y);
        this.belongPatch = belong;
    }

    public void move(Position2D dest) {
        position = dest;
    }

    @Override
    public Position2D position() {
        return position;
    }
}
