package edu.buffalo.cse.cse486586.simpledynamo;
/**************************************************************************************/
/**************** General Overview of the Implementation ******************************/
/**																					 **/
/** This project implements a small version of Dynamo, supporting read, write and    **/
/** delete operations in a distributed DHT while maintaining replica's and handling  **/
/** one node failure. A difference is though Dynamo implements eventual consistency  **/
/** this project implements linearizability and hence returns the latest write       **/
/** 																				 **/
/** Failure Handling:- Node failures are handled by continuing the operations with   **/
/** the other responsible nodes in case one node does not respond. In addition to    **/
/** socket exceptions, failure is detected by a null response from a remote node     **/
/** as in android, after port redirection, a client can connect to remote port even  **/
/** if this application is not running and hence do not get they typical socket      **/
/** connection timeout exception or read timeout exception							 **/
/**																					 **/
/** Also, whenever a node comes up, it does a sync operation with it's predecessors  **/
/** and successors, to synchronize any entries it missed while it was down			 **/
/**																					 **/
/** Consistency:- This application maintains consistency by ensuring following:-	 **/
/**																					 **/
/** 1.) Using singleton classes for Dynamo :- The classes implementing dynamo 		 **/
/** operations (DynamoOperation) or implementing the DHT topology (DynamoRing) 		 **/
/** are singleton. This ensures that even if they are invoked from different results,**/
/** they are not in inconsistent state and responds to queries from different threads**/
/** in a FIFO manner.																 **/
/**																					 **/
/** 2.) Ensuring that dynamo is initialized before responding to TCP requests		 **/
/**																					 **/
/** 3.) As content provider can be invoked from a different thread, it is ensured	 **/
/** in the content provider's thread that it will respond to dynamo queries only     **/
/** after dynamo is initialized.													 **/
/**																					 **/
/** Message Serialization:- For serialization, similar to all the previous projects, **/
/** messages are sent between the objects by serializing and deserializing the 		 **/
/** objects of Message class in it's JSON format.									 **/

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;

import android.os.AsyncTask;
import android.os.Bundle;
import android.app.Activity;
import android.content.Context;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.widget.TextView;

public class SimpleDynamoActivity extends Activity {
	
	static final String TAG = SimpleDynamoActivity.class.getSimpleName();
	private DynamoOperation dynamoOperation;		// Object which carries out all the Dynamo operations
	private static String myPort;					// Port string of this node
	
	private static int NODE_COUNT = 5;				// Number of nodes in the Dynamo DHT
	private static int REPLICATION_COUNT = 3;		// Number of replica's for a key value pair
	private static int READ_QUORUM = 2;				// Read quorum
	private static int WRITE_QUORUM = 2;			// Write quorum

	private final int SERVER_PORT = 10000;			// Port at which server is listening

	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.activity_simple_dynamo);
		
		TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
		myPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
    
		TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        
        try {
			/*
			 * Initializes dynamo object as well as start the server to respond to other clients.
			 * 
			 * AsyncTask is a simplified thread construct that Android provides. Please make sure
			 * you know how it works by reading
			 * http://developer.android.com/reference/android/os/AsyncTask.html
			 */
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new InitDynamoTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		} catch (IOException e) {
			/*
			 * Log is a good way to debug your code. LogCat prints out all the messages that
			 * Log class writes.
			 * 
			 * Please read http://developer.android.com/tools/debugging/debugging-projects.html
			 * and http://developer.android.com/tools/debugging/debugging-log.html
			 * for more information on debugging.
			 */
			Log.e(TAG, "Can't create a ServerSocket");
			return;
		}
	}

	@Override
	public boolean onCreateOptionsMenu(Menu menu) {
		// Inflate the menu; this adds items to the action bar if it is present.
		getMenuInflater().inflate(R.menu.simple_dynamo, menu);
		return true;
	}
	
	
	/**
	 * This class implements task to do two things:-
	 * 1.) Initializes dynamo.
	 * 2.) Creates a server socket and starts accepting incoming requests.
	 * 
	 * Also, it is necessary to do Step 1 before Step 2, so that the socket can respond
	 * to queries only after dynamo is initialized.
	 * 
	 * @author biplap
	 *
	 */
	private class InitDynamoTask extends AsyncTask<Object, String, Void> {
		
		@Override
		protected Void doInBackground(Object... params) {
			ServerSocket serverSocket = (ServerSocket) params[0];
			int myAddress = Integer.parseInt(myPort)*2;
			
			// Initialize dynamo
			dynamoOperation = DynamoOperation.createAndGetInstance(getApplicationContext(), myAddress, NODE_COUNT, REPLICATION_COUNT, READ_QUORUM, WRITE_QUORUM);
			
			// Can listen to incoming requests once dynamo is initialized
			while(true){		// Keep listening to incoming requests
				try {
					Socket soc = serverSocket.accept();
					Log.v(TAG, "Client connected");
					BufferedReader br = new BufferedReader(new InputStreamReader(soc.getInputStream()));
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(soc.getOutputStream()));
					String rawMsg = br.readLine();
					Message inMsg = Message.fromJson(rawMsg);
					Log.v(TAG, "Message type "+inMsg.getType());
					
					// Handling read request
					if(inMsg.getType() == Message.READ){
						
						String key = inMsg.getKey();
						ArrayList<KeyVal> keyValList = null;
						keyValList = dynamoOperation.readLocalKeyVal(key);
						Message reply = new Message();
						reply.setType(Message.READ_ACK);
						reply.setKeyValList(keyValList);
						String replyStr = reply.toJson();
						bw.write(replyStr+"\n");
						bw.flush();
					}
					
					// handling request to write a new key value
					else if(inMsg.getType() == Message.WRITE){
						Log.v(TAG, "Insert request received at "+myPort);
						dynamoOperation.writeLocalKeyVal(inMsg.getKey(), inMsg.getValue());
						Message response = new Message();
						response.setType(Message.WRITE_ACK);
						String responseStr = response.toJson();
						bw.write(responseStr+"\n");
						bw.flush();
					}
					
					// handling request to delete key value
					else if(inMsg.getType() == Message.DELETE){
						int res = dynamoOperation.deleteLocalKeyVal(inMsg.getKey());
						Message response = new Message();
						response.setType(Message.DELETE_ACK);
						response.setSqlResult(res);
				    	String responseStr = response.toJson();
				    	bw.write(responseStr+"\n");
				    	bw.flush();
					}
					br.close();
					bw.close();
					soc.close();
				} catch (IOException e) {
					Log.e(TAG, "ServerTask socket IOException");
				}
			}
		}
	}
}

