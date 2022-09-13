package handist.collections.patch;

import java.io.Serializable;

public class Index2D implements Serializable {

    private static final long serialVersionUID = 4021851641994578071L;

    public final int x, y;

    public Index2D(int x, int y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Index2D)) {
            return false;
        }
        final Index2D v = (Index2D) o;
        return (x == v.x && y == v.y);
    }

    @Override
    public String toString() {
        return "Vector2Int[" + x + "," + y + "]";
    }

    @Override
    public int hashCode() {
        // refer to LongRange#hashCode
        return (int) ((x << 4) + (x >> 16) + y);
    }
}
