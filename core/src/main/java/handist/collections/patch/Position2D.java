package handist.collections.patch;

import java.io.Serializable;

public class Position2D implements Serializable {

    private static final long serialVersionUID = 4021851641994578071L;

    public double x;
    public double y;

    public Position2D(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Position2D)) {
            return false;
        }
        final Position2D v = (Position2D) o;
        return (Double.compare(x, v.x) == 0 && Double.compare(y, v.y) == 0);
    }

    @Override
    public int hashCode() {
        // refer to
        // https://github.com/openjdk/jdk/blob/739769c8fc4b496f08a92225a12d07414537b6c0/src/java.desktop/share/classes/java/awt/geom/Point2D.java
        long bits = Double.doubleToLongBits(x);
        bits ^= Double.doubleToLongBits(y) * 31;
        return (((int) bits) ^ ((int) (bits >> 32)));
    }

    @Override
    public String toString() {
        return "Vector2[" + x + "," + y + "]";
    }
}
