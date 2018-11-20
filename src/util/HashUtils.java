package util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {
	private static MessageDigest md5;

	static {
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	public static String getHash(String key) {
		byte[] digest = md5.digest(key.getBytes(StandardCharsets.UTF_8));
		StringBuffer sb = new StringBuffer();
		for (byte b : digest) {
			sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
		}
		while (sb.length() < 32) {
			sb.insert(0, '0');
		}

		return sb.toString();
	}
	
	public static byte[] getHashBytes(String hashString) {
		String[] splitHashString = hashString.split("..");
		byte[] output = new byte[hashString.length()/2];
		for(int i = 0; i<splitHashString.length; i++) {
			output[i] = Byte.parseByte(splitHashString[i], 16);
		}
		return output;
	}
}
