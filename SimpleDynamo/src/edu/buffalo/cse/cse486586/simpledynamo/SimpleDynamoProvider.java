package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.ArrayList;


import android.content.ContentProvider;
import android.content.ContentValues;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.util.Log;

/**
 * This class implements the content provider to respond to 
 * Dynamo queries.
 * 
 * @author biplap
 *
 */
public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final int INIT_DYNAMO_WAIT_TIME = 100;			// Wait interval for waiting dynamo to be initialized
	private DynamoOperation dynamoOperation = null;	
	
	@Override
	/**
	 * Implements delete operation.
	 * Redirects to dynamo operation object for the implementation.
	 */
	public int delete(Uri uri, String selection, String[] selectionArgs) {	
		ensureDynamoInit(); 		// Make sure that dynamo is initialized
		int result = 0;
    	String key = selection;
    	result = dynamoOperation.deleteDHTKeyVal(key);
    	return result;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	/**
	 * Implements insert/write operation.
	 * Redirects to dynamo operation object for the implementation
	 */
	public Uri insert(Uri uri, ContentValues values) {
		ensureDynamoInit();			// Make sure that dynamo is initialized
		String key = values.getAsString(DBHelper.KEY_FIELD);
    	String val = values.getAsString(DBHelper.VALUE_FIELD);
    	dynamoOperation.writeDHTKeyVal(key, val);
		return uri;
	}

	@Override
	public boolean onCreate() {
		return true;
	}

	@Override
	/**
	 * Implements query/read operation.
	 * Redirects to dynamo operation object for the implementation
	 */
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		ensureDynamoInit();			// Make sure that dynamo is initialized
		ArrayList<KeyVal> keyValList = new ArrayList<KeyVal>();
		String key = selection;
    	if(key.equals("*"))
    		keyValList = dynamoOperation.readDHTAll();
    	else if(key.equals("@"))
    		keyValList = dynamoOperation.readAllLocal();
    	else{
    		keyValList = dynamoOperation.readDHTKeyVal(key);
    	}
    	Cursor resultCursor = buildCursorFromKeyValList(keyValList);
		return resultCursor;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	/**
	 * This mehod builds a Cursor from list of key value pairs
	 * returned from dynamo operation object to be returned as result of
	 * query()
	 *  
	 * @param keyValList
	 * @return
	 */
	private Cursor buildCursorFromKeyValList(ArrayList<KeyVal> keyValList){
		MatrixCursor matCursor = new MatrixCursor(new String[]{DBHelper.KEY_FIELD,DBHelper.VALUE_FIELD});
		matCursor.moveToFirst();
		for(int i=0;i<keyValList.size();i++){
			KeyVal keyVal = keyValList.get(i);
			matCursor.addRow(new String[]{keyVal.getKey(),keyVal.getVal()});
		}
		return matCursor;
	}
	
	/**
	 * Makes the current thread to wait till dynamo is initialized.
	 * This is important as handling queries before dynamo is initializes
	 * will result in consistency problems.
	 */
	private void ensureDynamoInit(){
		while(dynamoOperation == null){
			try {
				Thread.sleep(INIT_DYNAMO_WAIT_TIME);
				dynamoOperation = DynamoOperation.getInstance();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
    
}
