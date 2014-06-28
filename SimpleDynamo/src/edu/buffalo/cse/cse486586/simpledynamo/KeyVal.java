package edu.buffalo.cse.cse486586.simpledynamo;

/**
 * This class represents a key value pair of record.
 * This is same as that in PA3 with only difference is that
 * the key value pairs have versions too to determine latest
 * value of a key pair
 * 
 * @author biplap
 *
 */
public class KeyVal {

	private String key;			
	private String val;
	private String version;
	
	/**
	 * Returns the version
	 * @return
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * Sets the version
	 * @param version
	 */
	public void setVersion(String version) {
		this.version = version;
	}

	/**
	 * Returns the key
	 * @return
	 */
	public String getKey() {
		return key;
	}
	
	/**
	 * Sets the key
	 * @param key
	 */
	public void setKey(String key) {
		this.key = key;
	}
	
	/**
	 * Returns the value
	 * @return
	 */
	public String getVal() {
		return val;
	}
	
	/**
	 * Sets the value
	 * @param val
	 */
	public void setVal(String val) {
		this.val = val;
	}
	

}
