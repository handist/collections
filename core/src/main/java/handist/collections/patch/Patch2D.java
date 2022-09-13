package handist.collections.patch;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import com.hazelcast.util.function.BiConsumer;

/**
 *
 *
 * @author yoshikikawanishi
 *
 * @param <T>
 */
public class Patch2D<T extends Positionable<Position2D>> implements Serializable, Iterable<T> {

    private static final long serialVersionUID = 3967413332154266455L;

    /** the patch range */
    public final Range2D range;
    /** collection of elements */
    private final Collection<T> a;
    /** the number to identify the patch positioning among world range */
    int id;

    public Patch2D(Range2D range) {
        this.range = range;
        a = new HashSet<>(); // use HashSet because I think maybe many opportunities for random access when
                             // migrating elements. If not, please change the other collection.
    }

    public Patch2D(Position2D leftBottom, Position2D rightTop) {
        this(new Range2D(leftBottom, rightTop));
    }

    /**
     *
     */
    public void clear() {
        a.clear();
    }

    /**
     * @param t
     * @return
     */
    public boolean contains(T t) {
        return a.contains(t);
    }

    /**
     * @param func
     */
    public void forEach(BiConsumer<Integer, ? super T> func) {
        int i = 0;
        for (final T t : a) {
            func.accept(i, t);
            i++;
        }
    }

    /**
     * @param func
     */
    @Override
    public void forEach(Consumer<? super T> func) {
        a.forEach((T t) -> {
            func.accept(t);
        });
    }

    /**
     * @return
     */
    public Range2D getRange() {
        return range;
    }

    /**
     * The number to identify the patch positioning among world range. For the
     * {@link Patch2DList} case, each patch divided by the grid is assigned an ID
     * starting with ordered leftBottom to rightTop.
     */
    public int id() {
        return id;
    }

    @Override
    public Iterator<T> iterator() {
        return a.iterator();
    }

    /**
     *
     * @param pos
     * @param t
     */
    public void put(T t) {
        if (!range.contains(t.position())) {
            throw new IndexOutOfBoundsException("out of patch range (" + range + "): " + t);
        }
        a.add(t);
    }

    /**
     * @param point
     * @return
     */
    public boolean remove(T t) {
        return a.remove(t);
    }

    /**
     * Set the number to identify this patch positioning among all range. Basically,
     * this method will be called by only classes that are a collection of patches
     * like {@link Patch2DList}.
     *
     * @param id
     */
    @Deprecated // make visibility public in order to DistPatch2List
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @return
     */
    public int size() {
        return a.size();
    }

    /**
     * Return the split patches. Be careful that this method does copy all elements
     * from this patch to the split patches. Basically, this method will be called
     * by only classes that are a collection of patches like {@link Patch2DList}.
     *
     * @param xn
     * @param yn
     * @return
     */
    @Deprecated // make visibility public in order to DistPatch2List
    public Collection<Patch2D<T>> split(int xn, int yn) {
        final List<Patch2D<T>> result = new ArrayList<>();
        final Collection<Range2D> ranges = range.split(xn, yn);

        ranges.forEach((r) -> {
            final Patch2D<T> patch = new Patch2D<>(r);
            a.forEach((t) -> {
                if (patch.getRange().contains(t.position())) {
                    patch.put(t);
                }
            });
            result.add(patch);
        });
        return result;
    }

}
