package util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class HashUtils {
    public static final String MAX_HASH = new String(new char[8]).replace("\0", "ffff");
    public static final String MIN_HASH = new String(new char[8]).replace("\0", "0000");
    private static MessageDigest md5;

    static {
        try {
            md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    public static String getHash(String key) {
        byte[] digest = getHashBytes(key);
        return getHashStringOf(digest);
    }

    public static byte[] getHashBytesOf(String hashString) {
        String[] splitHashString = StringUtils.splitEvery(hashString, 2);
        byte[] output = new byte[splitHashString.length];
        for (int i = 0; i < splitHashString.length; i++)
            output[i] = (byte) Integer.parseInt(splitHashString[i], 16);
        return output;
    }

    public static byte[] getHashBytes(String key) {
        return md5.digest(key.getBytes(StandardCharsets.US_ASCII));
    }

    public static String getHashStringOf(byte[] digest) {
        StringBuffer sb = new StringBuffer();
        for (byte b : digest) {
            sb.append(Integer.toHexString((b & 0xFF) | 0x100).substring(1, 3));
        }

        while (sb.length() < 32) {
            sb.insert(0, '0');
        }
        return sb.toString().toLowerCase();
    }

    public static String increaseHashBy1 (String hashString) {
        byte[] hashBytes = getHashBytesOf(hashString);
        int i = hashBytes.length - 1;
        while (hashBytes[i] == 0xFF) {
            hashBytes[i] = 0x00;
            i--;
        }
        hashBytes[i] += 0x01;
        return getHashStringOf(hashBytes);
    }

    /**
     * Notice: This method is not optimized for running time and should be used only for testing purposes
     *
     * @param hashString
     * @param by
     * @return
     */
    public static String increaseHashBy (String hashString, int by) {
        for (int i = 1; i <= by ; i++) {
            hashString = increaseHashBy1(hashString);
        }
        return hashString;
    }


    /**
     * Compares 2 MD5 hashes represented as byte arrays
     *
     * @param hashA the first byte-array hash value
     * @param hashB the second byte-array hash value
     * @return 0 if {@param hashA} equals {@param hashB}. 1 if {@param hashA} is greater and -1 if {@param hashA} less than {@param hashB}.
     */
    public static short compare(byte[] hashA, byte[] hashB) {
        Validate.isTrue(hashA.length == hashB.length, "Length of 2 hash values do not match");

        for (int i = 0; i < hashA.length; i++) {
            if ((hashA[i] & 0xFF)  < (hashB[i] & 0xFF))
                return -1;

            else if ((hashA[i] & 0xFF)  > (hashB[i] & 0xFF))
                return 1;

        }
        return 0;
    }

}
