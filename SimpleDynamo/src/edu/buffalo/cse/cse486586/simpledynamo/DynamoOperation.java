package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.util.Log;
import android.util.SparseArray;

/**
 * This is a singleton class whose object implements all the dynamo level details
 * and manages the read, write, delete operations with replication and failure 
 * handling.
 * 
 * @author biplap
 *
 */
public class DynamoOperation {
	private static final String TAG = DynamoRing.class.getSimpleName();
	private static DynamoOperation dynamoOperation = null;
	private DynamoRing dynamoRing = null;
	private DBHelper dbHelper = null;
	private final int MY_ADDRESS;
	private final int REPLICATION_COUNT;
	private final int READ_QUORUM;
	private final int WRITE_QUORUM;
	
	
	/**
	 * Private constructor of the class to make it singleton
	 * @param context
	 * @param myAddress
	 * @param nodeCount
	 * @param replicationCount
	 * @param readQuorum
	 * @param writeQuorum
	 */
	private DynamoOperation(Context context, int myAddress, int nodeCount, int replicationCount, int readQuorum, int writeQuorum){
		dbHelper = new DBHelper(context);
		dynamoRing = DynamoRing.createAndGetInstance(nodeCount);
		MY_ADDRESS = myAddress;
		REPLICATION_COUNT = replicationCount;
		READ_QUORUM = readQuorum;
		WRITE_QUORUM = writeQuorum;
		sync();
	}
	
	/**
	 * This method creates and returns the single object of this class which will be
	 * alive throughout the execution of this program.
	 * 
	 * @param context:- Context of the activity which calls this method
	 * @param myAddress:- Address of this node
	 * @param nodeCount:- Total number of nodes in the DHT Ring
	 * @param replicationCount:- Replication count
	 * @param readQuorum:- Read quorum
	 * @param writeQuorum:- Write quorum
	 * 
	 * @return:- A singleton instance of this class 
	 */
	public static DynamoOperation createAndGetInstance(Context context, int myAddress, int nodeCount, int replicationCount, int readQuorum, int writeQuorum){
		dynamoOperation = new DynamoOperation(context, myAddress, nodeCount, replicationCount, readQuorum, writeQuorum);
		return dynamoOperation;
	}
	
	
	/**
	 * Returns the singleton instance of this class which was previously created
	 * using createAndGetInstance()
	 * 
	 * @return
	 */
	public static DynamoOperation getInstance(){
		return dynamoOperation;
	}
	
	/**
	 * Reads a key value pair from the local node
	 * @param key:- key of the key value pair
	 * @return List of key value pairs whose key is supplied as argument 
	 */
	public ArrayList<KeyVal> readLocalKeyVal(String key){
		ArrayList<KeyVal> keyValList = new ArrayList<KeyVal>();
		String querySelection = DBHelper.KEY_FIELD+"=?";
		String selection = key;
		String []querySelectionArgs = new String[]{selection};
		if(selection.equals("@")){	// Fetch all the records
			querySelection = null;
			querySelectionArgs = null;
		}
		Cursor cursor = dbHelper.query(null, querySelection, querySelectionArgs, null);
		if (cursor.moveToFirst()){
			do{
				KeyVal newKeyVal = new KeyVal();
				newKeyVal.setKey(cursor.getString(0));
				newKeyVal.setVal(cursor.getString(1));
				newKeyVal.setVersion(cursor.getString(2));
				keyValList.add(newKeyVal);
			}while(cursor.moveToNext());
		}
		cursor.close();
		return keyValList;
	}
	
	/**
	 * Writes a key value pair in the local node.
	 * The version will be updated to be the newest one ie.
	 * the version will be one more than the max version of the key value pair
	 * if it existed earlier
	 * 
	 * @param key
	 * @param val
	 */
	public void writeLocalKeyVal(String key, String val){
		String version = "";
		ArrayList<KeyVal> oldEntry = readLocalKeyVal(key);
		if(oldEntry.size()==0)
			version = String.valueOf(1);
		else{
			int oldVersion = Integer.parseInt(oldEntry.get(0).getVersion());
			version = String.valueOf(oldVersion+1);
		}
		writeLocalKeyVal(key, val, version);
	}
	
	/**
	 * Writes a key value pair in the local node along with 
	 * the version supplied.
	 * 
	 * @param key
	 * @param val
	 * @param version
	 */
	public void writeLocalKeyVal(String key, String val, String version){
		ContentValues values = new ContentValues();
		values.put(DBHelper.KEY_FIELD, key);
		values.put(DBHelper.VALUE_FIELD, val);
		values.put(DBHelper.VERSION_FIELD, version);
		dbHelper.insert(values);
	}
	
	/**
	 * Reads a key value pair from the distributed hash table
	 * with failure handling.
	 * Normally failure detection should be done using socket exceptions
	 * but I observed that after port redirection, even if no application is
	 * running on the emulator, connect succeeds and there is no connection timed out
	 * exception. Even write to the socket succeeds. The only way to determine
	 * failure is when reading to socket returns null. 
	 * 
	 * @param key
	 * @return
	 */
	public ArrayList<KeyVal> readDHTKeyVal(String key){
		DHTNode node = dynamoRing.getResponsibleNode(key);
		ArrayList<DHTNode> successorList = dynamoRing.getNSuccessors(node, REPLICATION_COUNT-1);
		ArrayList<DHTNode> responsibleNodeList = successorList;
		HashMap<String, KeyVal> resultMap = new HashMap<String, KeyVal>();
		ArrayList<KeyVal> resultList = new ArrayList<KeyVal>();
		responsibleNodeList.add(node);
		for(int i=0;i<responsibleNodeList.size();i++){
			try{	// try inside the for loop so as to continue fetching data from other
					// responsible nodes even if one node fails to return any value.
				DHTNode nextNode = responsibleNodeList.get(i);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						nextNode.getAddress());
				Log.v(TAG, "Connected with "+nextNode.getAddress());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				Message msg = new Message();
				msg.setType(Message.READ);
				msg.setKey(key);
				String msgStr = msg.toJson();
				bw.write(msgStr+"\n");
				bw.flush();
				String responseStr = br.readLine();
				Message response = Message.fromJson(responseStr);
				if(response == null)	// null response, the remote node must have failed
										// continue fetching from the next node
					continue;
				ArrayList<KeyVal> keyValList = response.getKeyValList();
				for(int j=0;j<keyValList.size();j++){
					KeyVal keyVal = keyValList.get(j);
					if(resultMap.get(keyVal.getKey())==null){
						resultMap.put(keyVal.getKey(), keyVal);
					}
					else{
						int oldVersion = Integer.parseInt(resultMap.get(keyVal.getKey()).getVersion());
						int newVersion = Integer.parseInt(keyVal.getVersion());
						if(newVersion > oldVersion)
							resultMap.put(keyVal.getKey(), keyVal);
					}
				}
				bw.close();
				br.close();
				socket.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		Iterator<Entry<String, KeyVal>> resultIterator = resultMap.entrySet().iterator();
		while(resultIterator.hasNext()){
			Entry<String, KeyVal> nextEntry = resultIterator.next();
			resultList.add(nextEntry.getValue());
		}
		return resultList;
	}
	
	/**
	 * Writes a key value pair to the distributed hash table
	 * with failure handling.
	 * Similar to read, I observed that after port redirection, even if no application is
	 * running on the emulator, connect succeeds and there is no connection timed out
	 * exception. Even write to the socket succeeds. The only way to determine
	 * failure is when reading to response from remote node socket returns null. 
	 * 
	 * @param key
	 * @param val
	 */
	public void writeDHTKeyVal(String key, String val){
		DHTNode node = dynamoRing.getResponsibleNode(key);
		ArrayList<DHTNode> successorList = dynamoRing.getNSuccessors(node, REPLICATION_COUNT-1);
		ArrayList<DHTNode> responsibleNodeList = successorList;
		responsibleNodeList.add(node);
		for(int i=0;i<responsibleNodeList.size();i++){
			try{		// try block inside the for loop so as to continue writing data to other
						// responsible nodes even if one node fails
				DHTNode nextNode = responsibleNodeList.get(i);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						nextNode.getAddress());
				Log.v(TAG, "Connected with "+nextNode.getAddress());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				Message msg = new Message();
				msg.setType(Message.WRITE);
				msg.setKey(key);
				msg.setValue(val);
				String msgStr = msg.toJson();
				bw.write(msgStr+"\n");
				bw.flush();
				String responseStr = br.readLine();
				Message response = Message.fromJson(responseStr);
				if(response == null)		// null returned, remote node must have failed
											// continue writing to next responsible node
					continue;
				bw.close();
				br.close();
				socket.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Reads all key value pairs from the specified node
	 * 
	 * @param node:- DHT node from where all key value pairs have to be read
	 * 
	 * @return List of key value pairs
	 */
	public ArrayList<KeyVal> readDHTAllFromNode(DHTNode node){
		ArrayList<KeyVal> keyValList = new ArrayList<KeyVal>();
		try{
			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
					node.getAddress());
			Log.v(TAG, "Connected with "+node.getAddress());
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
			BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			Message msg = new Message();
			msg.setType(Message.READ);
			msg.setKey("@");
			String msgStr = msg.toJson();
			bw.write(msgStr+"\n");
			bw.flush();
			String responseStr = br.readLine();
			bw.close();
			br.close();
			socket.close();
			Message response = Message.fromJson(responseStr);
			if(response != null)
				keyValList = response.getKeyValList();
			return keyValList;
			
		}
		catch(Exception e){
			e.printStackTrace();
		}
		return keyValList;
	}
	
	/**
	 * Reads all key value pairs from the local node
	 * @return
	 */
	public ArrayList<KeyVal> readAllLocal(){
		DHTNode myNode = dynamoRing.getNode(MY_ADDRESS);
		return readDHTAllFromNode(myNode);
	}
	
	/**
	 * Reads all key value pairs in the Dynamo DHT, ie all the 
	 * nodes in the DHT ring
	 * 
	 * @return
	 */
	public ArrayList<KeyVal> readDHTAll(){
		ArrayList<DHTNode> nodeList = dynamoRing.getAllNodes();
		HashMap<String, KeyVal> resultMap = new HashMap<String, KeyVal>();
		ArrayList<KeyVal> resultList = new ArrayList<KeyVal>();
		for(int i=0;i<nodeList.size();i++){
			ArrayList<KeyVal> keyValList = readDHTAllFromNode(nodeList.get(i));
			for(int j=0;j<keyValList.size();j++){
				KeyVal keyVal = keyValList.get(j);
				if(resultMap.get(keyVal.getKey())==null){
					resultMap.put(keyVal.getKey(), keyVal);
				}
				else{
					int oldVersion = Integer.parseInt(resultMap.get(keyVal.getKey()).getVersion());
					int newVersion = Integer.parseInt(keyVal.getVersion());
					if(newVersion > oldVersion)
						resultMap.put(keyVal.getKey(), keyVal);
				}
			}
		}
		Iterator<Entry<String, KeyVal>> resultIterator = resultMap.entrySet().iterator();
		while(resultIterator.hasNext()){
			Entry<String, KeyVal> nextEntry = resultIterator.next();
			resultList.add(nextEntry.getValue());
		}
		return resultList;
	}
	
	
	/**
	 * Deletes a key value pair from the local node specified by the key.
	 * @param key
	 * @return
	 */
	public int deleteLocalKeyVal(String key){
		int result = 0;
		String whereClause = DBHelper.KEY_FIELD+"=?";
		String selection = key;
    	String []whereArgs = new String[]{selection};
    	if(selection.equals("@")){
    		whereClause = "1";
    		whereArgs = null;
    	}
    	result = dbHelper.delete(whereClause, whereArgs);
		return result;
	}
	
	/**
	 * Deletes a key value pair from the DHT.
	 * Sends delete request to all the responsible nodes for the key.
	 * If one request fails, it continues to send delete requests to other nodes.
	 * 
	 * @param key
	 * @return
	 */
	public int deleteDHTKeyVal(String key){
		DHTNode node = dynamoRing.getResponsibleNode(key);
		ArrayList<DHTNode> successorList = dynamoRing.getNSuccessors(node, REPLICATION_COUNT-1);
		ArrayList<DHTNode> responsibleNodeList = successorList;
		int result = 0;
		responsibleNodeList.add(node);
		for(int i=0;i<responsibleNodeList.size();i++){
			try{			// try block inside the for loop so as to continue sending delete request to other
							// responsible nodes even if one node fails
				DHTNode nextNode = responsibleNodeList.get(i);
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						nextNode.getAddress());
				Log.v(TAG, "Connected with "+nextNode.getAddress());
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				Message msg = new Message();
				msg.setType(Message.DELETE);
				msg.setKey(key);
				String msgStr = msg.toJson();
				bw.write(msgStr+"\n");
				bw.flush();
				String responseStr = br.readLine();
				Message response = Message.fromJson(responseStr);
				if(response == null)	// response is null, the remote node must have failed
										// continue sending delete requests to other responsible nodes
					continue;
				if(response.getSqlResult()>result)
					result = response.getSqlResult();
				bw.close();
				br.close();
				socket.close();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
		
		return result;
	}
	
	/**
	 * This method synchronizes the entries at a node from it's successors and predecessors.
	 * More specifically, this node has to store all key value pairs for whom this node and it's N-1 predecessors
	 * are responsible where N is the total number of replica. For this, it contacts it's N-1 predecessors
	 * and N-1 successors.
	 * 
	 * This method is called whenever the singleton object of this class is created, ie whenever Dynamo
	 * is initialized. If the node is coming back after a crash, it gets all it's previous entries. 
	 * If the node is started for first time, it doesn't get any results.
	 * 
	 */
	public void sync(){
		DHTNode myNode = dynamoRing.getNode(MY_ADDRESS);
		ArrayList<DHTNode> dependentNodes = dynamoRing.getNPredecessors(myNode, REPLICATION_COUNT-1);
		dependentNodes.add(myNode);
		SparseArray<String> dependentNodeMap = new SparseArray<String>();
		HashMap<String, KeyVal> resultMap = new HashMap<String, KeyVal>();
		for(int i=0;i<dependentNodes.size();i++)	// Ideally it should contact only N-1 successors and N-1 predecessors
													// but for total node count = 5 and replica count = 2, this
													// number spans through all the nodes
			dependentNodeMap.put(dependentNodes.get(i).getAddress(), dependentNodes.get(i).getId());
		
		ArrayList<DHTNode> nodeList = dynamoRing.getAllNodes();
		for(int i=0;i<nodeList.size();i++){
			if(nodeList.get(i)==myNode)
				continue;
			ArrayList<KeyVal> keyValList = readDHTAllFromNode(nodeList.get(i));
			for(int j=0;j<keyValList.size();j++){
				KeyVal keyVal = keyValList.get(j);
				int responsibleNodeAddress = dynamoRing.getResponsibleNode(keyVal.getKey()).getAddress();
				if(dependentNodeMap.get(responsibleNodeAddress)==null)
					continue;
				else{
					if(resultMap.get(keyVal.getKey())==null){
						resultMap.put(keyVal.getKey(), keyVal);
					}
					else{
						int oldVersion = Integer.parseInt(resultMap.get(keyVal.getKey()).getVersion());
						int newVersion = Integer.parseInt(keyVal.getVersion());
						if(newVersion > oldVersion)
							resultMap.put(keyVal.getKey(), keyVal);
					}
				}
			}
		}
		
		Iterator<Entry<String, KeyVal>> resultIterator = resultMap.entrySet().iterator();
		while(resultIterator.hasNext()){
			Entry<String, KeyVal> nextEntry = resultIterator.next();
			KeyVal keyVal = nextEntry.getValue();
			writeLocalKeyVal(keyVal.getKey(), keyVal.getVal(), keyVal.getVersion());
		}
		
	}
}
