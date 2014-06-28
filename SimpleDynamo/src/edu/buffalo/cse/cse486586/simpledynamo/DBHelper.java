package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

/**
 * This class implements database level details
 * to insert, query and delete key value pairs
 * while maintaining their versions.
 * 
 * @author biplap
 *
 */
public class DBHelper extends SQLiteOpenHelper{	
	public static final int DB_VERSION = 1;
	public static final String DBNAME = "simpledynamo";
	public static final String TABLE_NAME = "keyval";
	public static final String KEY_FIELD = "key";
	public static final String VALUE_FIELD = "value";
	public static final String VERSION_FIELD = "version";

	/* SQL statement to create the table */
	private static final String CREATE_TABLE = "CREATE TABLE "+ TABLE_NAME + "( " +
			KEY_FIELD+" TEXT PRIMARY KEY, " + 
			VALUE_FIELD+" TEXT ,"+VERSION_FIELD+" TEXT ) ";

	/**
	 * Default constructor for given context
	 * @param context
	 */
	public DBHelper(Context context) {
		super(context, DBNAME, null, DB_VERSION);
	}

	/**
	 * Creates table, drops table before that if present previously.
	 * This simulates a complete crash of a node, ie if a node crashes,
	 * it's data is lost too.
	 */
	@Override
	public void onCreate(SQLiteDatabase db) {
		Log.v("Log", "Creating Table");
		db.execSQL("DROP TABLE IF EXISTS "+TABLE_NAME);
		db.execSQL(CREATE_TABLE);
	}

	/**
	 * Creates table on upgrade
	 */
	@Override
	public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
		Log.v("Log", "Upgrading Table");
		this.onCreate(db);
	}

	/**
	 * Inserts a new key value pair in the table
	 * @param value
	 */
	public void insert(ContentValues value){
		SQLiteDatabase db = this.getWritableDatabase();
		db.insertWithOnConflict(TABLE_NAME, null, value, SQLiteDatabase.CONFLICT_REPLACE);
		Log.v("dbhelper", "inserted key="+value.getAsString("key")+" value="+value.getAsString("value"));
		db.close();
	}

	/**
	 * Queries the table for given key and returns a Cursor object of the result
	 * 
	 * @param projection
	 * @param selection
	 * @param selectionArgs
	 * @param sortOrder
	 * @return
	 */
	public Cursor query(String[] projection, String selection, String[] selectionArgs,
			String sortOrder){
		Cursor res = null;
		String []column = new String[1];
		column[0] = KEY_FIELD;
		SQLiteDatabase db = this.getReadableDatabase();
		res = db.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
		res.moveToFirst();
		db.close();
		return res;
	}
	
	/**
	 * Deletes a key value pair
	 * @param whereClause
	 * @param whereArgs
	 * @return
	 */
	public int delete(String whereClause, String[] whereArgs){
		SQLiteDatabase db = this.getReadableDatabase();
		return db.delete(TABLE_NAME, whereClause, whereArgs);
	}


}
