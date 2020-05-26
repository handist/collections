/**
 *
 */
package handist.collections;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Patrick
 */
public class BagTest {

  // instance provided for test
  Bag<Integer> b = null;
  Integer i = null;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    b = new Bag<>();
    i = new Integer(42);
  }

  @Test
  public void testContains() {
    assertFalse("Integer was not placed in the Bag, should be false", b.contains(i));

    b.getReceiver().accept(i);
    assertTrue("Integer i was placed, should be present", b.contains(i));

    b.clear();
    assertFalse("Bag was cleared, should be false", b.contains(i));
  }

  @Test
  public void testEmpty() {
    assertTrue("New bag should be empty", b.isEmpty());

    b.getReceiver().accept(i);
    assertFalse("An element was inserted, should NOT be empty", b.isEmpty());

    b.clear();
    assertTrue("Bag was cleared, should be empty again", b.isEmpty());
  }

}
