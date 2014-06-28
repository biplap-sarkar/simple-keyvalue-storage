package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.NoSuchAlgorithmException;

/**
 * This class represents a node in the Dynamo DHT.
 * 
 * @author biplap
 *
 */
public class DHTNode implements Comparable<DHTNode>{
	private String id;		// Id of the node which is sha1 hash of the address
	private int address;	// Address of the node. In this case since we are
							// testing this app on same machine with different emulators,
							// ports are used as address. In general IP can be used as address
	
	/**
	 * Public constructor
	 * @param address
	 */
	public DHTNode(int address){
		this.address = address;
		try {
			this.id = SHA1Helper.genHash(String.valueOf(address/2));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * This method gets the node id
	 * @return
	 */
	public String getId() {
		return id;
	}
	
	/**
	 * This method gets the address of the node id
	 * @return
	 */
	public int getAddress() {
		return address;
	}
	
	/**
	 * This function implements a logical order between two DHTNode objects.
	 * In our case this is just the lexicographic order of the id's of the nodes.
	 */
	@Override
	public int compareTo(DHTNode another) {
		return this.id.compareTo(another.getId());
	}
	
	
}
