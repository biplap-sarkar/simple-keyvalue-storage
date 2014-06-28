package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;

import com.google.gson.Gson;

/**
 * This class represents a message sent between the nodes in the Dynamo DHT.
 * It uses google's gson library to serialize itself by converting object to json format and vice versa.
 * 
 * @author biplap
 *
 */
public class Message {
	public static final int READ = 1;				// Type for a read request
	public static final int READ_ACK = 2;			// Type for a read response
	public static final int WRITE = 3;				// Type for a write request
	public static final int WRITE_ACK = 4;			// Type for a write response
	public static final int DELETE = 5;				// Type for a delete request
	public static final int DELETE_ACK = 6;			// Type for a delete response
		
	private int type;			// Type of the message
	private String key;			// Key of the record (used in write request)
	private String value;		// Value of the record (used in write request)
	ArrayList<KeyVal> keyValList;	// Key value list (used in read request)
	private int sqlResult;		// Sql result (used in delete request)
	
	/**
	 * Returns the type of the message
	 * @return
	 */
	public int getType() {
		return type;
	}

	/**
	 * Sets the type of the message
	 * @param type
	 */
	public void setType(int type) {
		this.type = type;
	}
	


	/**
	 * Returns the key in the message
	 * @return
	 */
	public String getKey() {
		return key;
	}

	/**
	 * Sets the key in the message
	 * @param key
	 */
	public void setKey(String key) {
		this.key = key;
	}

	/**
	 * Returns the value in the message
	 * @return
	 */
	public String getValue() {
		return value;
	}

	/**
	 * Sets the value in the message
	 * @param value
	 */
	public void setValue(String value) {
		this.value = value;
	}

	/**
	 * Returns the list of key value pairs in the message
	 * @return
	 */
	public ArrayList<KeyVal> getKeyValList() {
		return keyValList;
	}

	/**
	 * Sets the value of the key value pairs in the message
	 * @param keyValList
	 */
	public void setKeyValList(ArrayList<KeyVal> keyValList) {
		this.keyValList = keyValList;
	}

	/**
	 * Returns the sql result in the message
	 * @return
	 */
	public int getSqlResult() {
		return sqlResult;
	}

	/**
	 * Sets the sql result in the message
	 * @param sqlResult
	 */
	public void setSqlResult(int sqlResult) {
		this.sqlResult = sqlResult;
	}

	/**
	 * Serializes Message object to it's Json representation
	 * Refer https://code.google.com/p/google-gson/
	 * 
	 * @return Json representation of the object
	 */
	public String toJson(){
		Gson gson = new Gson();
		String jsonString = gson.toJson(this);
		return jsonString;
	}
	
	/**
	 * Deserializes Message object from it's Json representation
	 * Refer https://code.google.com/p/google-gson/
	 * 
	 * @param jsonString	Json string representing the Message Object
	 * @return	Message object
	 */
	public static Message fromJson(String jsonString){
		Gson gson = new Gson();
		Message message = gson.fromJson(jsonString, Message.class);
		return message;
	}
}
