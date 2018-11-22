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
        byte[] digest = md5.digest(key.getBytes(StandardCharsets.US_ASCII));
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
        byte[] output = new byte[hashString.length() / 2];
        for (int i = 0; i < splitHashString.length; i++) {
            output[i] = Byte.parseByte(splitHashString[i], 16);
        }
        return output;
    }

    /**
     * Compares 2 MD5 hashes represented as byte arrays
     *
     * @param hashA the first hash value
     * @param hashB the second hash value
     * @return 0 if {@param hashA} equals {@param hashB}. 1 if {@param hashA} is greater and -1 if {@param hashA} less than {@param hashB}.
     */
    public static short compare(byte[] hashA, byte[] hashB) {
        Validate.isTrue(hashA.length == hashB.length, "Length of 2 hash values do not match");

        for (int i = 0; i < hashA.length; i++) {
            if (hashA[i] < hashB[i])
                return -1;

            else if (hashA[i] > hashB[i])
                return 1;

        }
        return 0;
    }
}
