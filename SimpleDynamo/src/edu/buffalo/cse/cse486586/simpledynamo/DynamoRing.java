package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * This is a singleton class which provides abstraction for the DHT Ring of
 * Dynamo. This class has information regarding complete topology of the 
 * Dynamo ring and provides services to DynamoOperation class.
 * 
 * @author biplap
 *
 */
public class DynamoRing {
	private static DynamoRing dynamoRing = null;
	private static final String TAG = DynamoRing.class.getSimpleName();
	private static final int BASE_ADDRESS = 11108;
	private int nodeCount;
	ArrayList<DHTNode> nodeList = new ArrayList<DHTNode>();
	
	/**
	 * Private constructor to initialize dynamo ring with specified
	 * number of nodes
	 * 
	 * @param nodeCount:- number of nodes in Dynamo DHT
	 */
	private DynamoRing(int nodeCount){
		this.nodeCount = nodeCount;
		initDynamoRing();
	}
	
	/**
	 * Initialize the dynamo ring
	 */
	private void initDynamoRing(){
		for(int i=0;i<nodeCount;i++){
			nodeList.add(new DHTNode(BASE_ADDRESS+(i*4)));
		}
		Collections.sort(nodeList);
	}
	
	/**
	 * This method instantiates and returns the single instance
	 * of this class which will be used throughout the application
	 * 
	 * @param nodeCount:- number of nodes in the dynamo DHT
	 * @return Singleton instance of this class
	 */
	public static DynamoRing createAndGetInstance(int nodeCount){
		dynamoRing = new DynamoRing(nodeCount);
		return dynamoRing;
	}
	
	/**
	 * This method returns the instance of this class which was 
	 * created earlier using createAndGetInstance(int) method
	 * @return
	 */
	public static DynamoRing getInstance(){
		return dynamoRing;
	}
	
	/**
	 * This method returns a node which is responsible for keeping
	 * a given key
	 * 
	 * @param key
	 * @return
	 */
	public DHTNode getResponsibleNode(String key){
		try {
			String dhtHash = SHA1Helper.genHash(key);
			for(int i=0;i<nodeCount;i++){
				if(dhtHash.compareTo(nodeList.get(i).getId())<0)
					return nodeList.get(i);
			}
			return nodeList.get(0);
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * This method returns specified number of successors of a given node
	 * 
	 * @param node:- DHT node for which successors are to be found
	 * @param count:- Number of successors
	 * 
	 * @return:- List of successors
	 */
	public ArrayList<DHTNode> getNSuccessors(DHTNode node, int count){
		ArrayList<DHTNode> successors = new ArrayList<DHTNode>();
		int nodeIndex = -1;
		for(int i=0;i<nodeCount;i++){
			if(node.getId().equals(nodeList.get(i).getId())){
				nodeIndex = i;
				break;
			}
		}
		int nodesAdded = 0;
		while(nodesAdded<count){
			nodeIndex=(nodeIndex+1)%nodeCount;
			successors.add(nodeList.get(nodeIndex));
			nodesAdded = nodesAdded + 1;
		}
		return successors;
	}
	
	/**
	 * This method returns the list of all the nodes in the DHT Ring
	 * @return
	 */
	public ArrayList<DHTNode> getAllNodes(){
		return nodeList;
	}
	
	/**
	 * This method returns specified number of predecessors of a given node
	 * 
	 * @param node:- DHT node for which predecessors are to be found
	 * @param count:- Number of predecessors needed
	 * 
	 * @return:- List of predecessors
	 */
	public ArrayList<DHTNode> getNPredecessors(DHTNode node, int count){
		ArrayList<DHTNode> predecessors = new ArrayList<DHTNode>();
		int nodeIndex = -1;
		for(int i=0;i<nodeCount;i++){
			if(node.getId().equals(nodeList.get(i).getId())){
				nodeIndex = i;
				break;
			}
		}
		int nodesAdded = 0;
		while(nodesAdded<count){
			nodeIndex=nodeIndex-1;
			if(nodeIndex < 0)
				nodeIndex = nodeCount - 1;
			predecessors.add(nodeList.get(nodeIndex));
			nodesAdded = nodesAdded + 1;
		}
		return predecessors;
	}
	
	/**
	 * Returns a DHT Node which has the specified address.
	 * 
	 * @param address:- Address of the DHT Node
	 * 
	 * @return:- DHT Node
	 */
	public DHTNode getNode(int address){
		for(int i=0;i<nodeCount;i++)
			if(nodeList.get(i).getAddress()==address)
				return nodeList.get(i);
		return null;
	}

}
