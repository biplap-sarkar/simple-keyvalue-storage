package edu.buffalo.cse.cse486586.simpledynamo;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

/**
 * This is a helper class containing method to generate SHA1 Hash
 * @author biplap
 *
 */
public class SHA1Helper {
	
	/**
	 * This method generates sha1 hash for a given input string
	 * 
	 * @param input	:- String for which sha1 hash is to be generated
	 * @return:- sha1 hash as a string
	 * 
	 * @throws NoSuchAlgorithmException
	 */
	public static String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }
}
