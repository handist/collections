package handist.collections.glb;

import java.io.Serializable;

/**
 * Abstract handle gathering common operations and routine to all GLB
 * handles of distributed collections
 * @author Patrick Finnerty
 *
 */
public class AbstractGlbHandle implements Serializable {

	/** Serial Version UID */
	private static final long serialVersionUID = -4684225091595493661L;

	/**
	 * Checks if the static glb instance of class {@link GlobalLoadBalancer} is
	 * initialized and returns it. If there isn't a glb program being run, will
	 * throw an {@link IllegalStateException}
	 * @return the instance handling the current GLB program
	 * @throws IllegalStateException if there isn't any 
	 * {@link GlobalLoadBalancer#underGLB(apgas.SerializableJob)} method being
	 * called
	 */
	protected GlobalLoadBalancer getGlb() {
		final GlobalLoadBalancer glb = GlobalLoadBalancer.glb;
		if (glb == null) {
			throw new IllegalStateException("Cannot call method from outside GlobalLoadBalancer#underGlb method");
		} else {
			return glb;
		}
	}
}
