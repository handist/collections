package handist.collections.patch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class Range2D implements Serializable {

    private static final long serialVersionUID = 5783510675557270334L;

    public final Position2D leftBottom, rightTop;

    /**
     * [leftBottom, rightTop)
     */
    public Range2D(Position2D leftBottom, Position2D rightTop) {
        this.leftBottom = leftBottom;
        this.rightTop = rightTop;
    }

    /**
     * @param point
     * @return
     */
    public boolean contains(Position2D point) {
        if (point.x < leftBottom.x || point.x > rightTop.x || Double.compare(rightTop.x, point.x) == 0) {
            return false;
        }
        if (point.y < leftBottom.y || point.y > rightTop.y || Double.compare(rightTop.y, point.y) == 0) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (!(o instanceof Range2D)) {
            return false;
        }
        final Range2D r = (Range2D) o;
        return (r.leftBottom.equals(leftBottom) && r.rightTop.equals(rightTop));
    }

    /**
     * @return
     */
    public Position2D size() {
        return new Position2D(rightTop.x - leftBottom.x, rightTop.y - leftBottom.y);
    }

    /**
     * @param xn
     * @param yn
     * @return
     */
    public Collection<Range2D> split(int xn, int yn) {
        final List<Range2D> result = new ArrayList<>();
        final Position2D size = new Position2D((rightTop.x - leftBottom.x) / xn, (rightTop.y - leftBottom.y) / yn);

        for (int y = 0; y < yn; y++) {
            for (int x = 0; x < xn; x++) {
                final Position2D lb = new Position2D(leftBottom.x + size.x * x, leftBottom.y + size.y * y);
                final Position2D rt = new Position2D(lb.x + size.x, lb.y + size.y);
                result.add(new Range2D(lb, rt));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "[" + leftBottom + "," + rightTop + "]";
    }
}
