package handist.collections.dist;

import java.io.Serializable;

/**
 * Class used as element in the tests of the distributed collections
 */
class Element implements Serializable {
	/** Serial Version UID */
	private static final long serialVersionUID = -2659467143487621997L;
	
	/** String contained in the element */
	String s;
	
	/**
	 * Constructor with initial String value
	 * @param string initial value of the contained string, may be null
	 */
	Element(String string) {
		s = string;
	}
}