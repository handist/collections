package handist.collections.glb;

import static apgas.Constructs.*;
import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.HashMap;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import handist.collections.LongRange;
import handist.collections.dist.DistChunkedList;
import handist.collections.dist.DistributedCollection;
import handist.collections.dist.TeamedPlaceGroup;
import handist.mpijunit.MpiConfig;
import handist.mpijunit.MpiRunner;
import handist.mpijunit.launcher.TestLauncher;

/**
 * This class checks whether some objects are suitable to be used as keys into
 * maps. As part of the GLB implementation, a number of {@link HashMap}s are
 * used to contain various information either related to a certain
 * {@link DistributedCollection} or a {@link GlbOperation}. The difficulty
 * arises when objects are serialized to a place and then serialized back to
 * their original place as part of lambda expressions. This can cause problems
 * as the twice-serialized objects will be interpreted as a different key if
 * their object's {@link #equals(Object)} and {@link #hashCode()} methods are
 * not implemented correctly.
 *
 * @author Patrick Finnerty
 *
 */
@SuppressWarnings("javadoc")
@RunWith(MpiRunner.class)
@MpiConfig(ranks = 2, launcher = TestLauncher.class)
public class IT_ObjectsUsedAsMapKeys implements Serializable {

    /** Serial Version UID */
    private static final long serialVersionUID = -6260636733862383978L;

    /** Map whose keys are instances of GlbOperation */
    @SuppressWarnings("rawtypes")
    transient static HashMap<GlbOperation, Integer> glbOperationMap;

    /** Map whose keys are instances of distributed collections */
    transient static HashMap<Object, Integer> distributedCollectionMap;

    @BeforeClass
    public static void before() throws Exception {
        TeamedPlaceGroup.getWorld().broadcastFlat(() -> {
            System.setProperty("apgas.serialization", "true");
        });
    }

    @Before
    public void setUp() throws Exception {
        glbOperationMap = new HashMap<>();
        distributedCollectionMap = new HashMap<>();
    }

    @Test
    public void testCollectionAsKey() {
        final DistChunkedList<Element> collection = new DistChunkedList<>();
        distributedCollectionMap.put(collection, new Integer(42));
        finish(() -> {
            asyncAt(place(1), () -> {
                @SuppressWarnings("rawtypes")
                final DistChunkedList c = collection;
                asyncAt(place(0), () -> {
                    assertNotNull(IT_ObjectsUsedAsMapKeys.distributedCollectionMap.get(c));
                    assertTrue(IT_ObjectsUsedAsMapKeys.distributedCollectionMap.containsKey(c));
                });
            });
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testGlbOperationAsKey() throws Exception {
        final DistChunkedList<Element> collection = new DistChunkedList<>();
        final GlbOperation<DistChunkedList<Element>, Element, LongRange, LongRange, DistChunkedList<Element>> operationKey = new GlbOperation(
                collection, (a, b) -> {
                    ;
                }, new DistFuture(collection), () -> {
                    return null;
                }, null, null);
        glbOperationMap.put(operationKey, new Integer(43));
        assertNotNull(operationKey);

        finish(() -> {
            asyncAt(place(1), () -> {
                final GlbOperation op = operationKey;
                asyncAt(place(0), () -> {
                    assertNotNull(IT_ObjectsUsedAsMapKeys.glbOperationMap.get(op));
                    assertTrue(IT_ObjectsUsedAsMapKeys.glbOperationMap.containsKey(op));
                });
            });
        });
    }

}
