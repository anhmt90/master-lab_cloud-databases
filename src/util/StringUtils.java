package util;

import java.util.Arrays;

public class StringUtils {

    public static final char LINE_FEED = 0x0A;
    public static final char RETURN = 0x0D;
    public static final String EMPTY_STRING = "";
    public static final String WHITE_SPACE = " ";


    /**
     * Convert a string to its byte array representation with a line feed and carriage return at the end
     *
     * @param string string to be converted
     * @return
     */
    public static byte[] toByteArray(String string) {
        byte[] bytes = string.getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public static String getLongestCommonPrefix(String strA, String strB) {
        int minLength = Math.min(strA.length(), strB.length());
        for (int i = 0; i < minLength; i++)
            if (strA.charAt(i) != strB.charAt(i))
                return strA.substring(0, i);
        return strA.substring(0, minLength);
    }

    public static String[] splitEvery(String str, int n) {
        if (n <= 0)
            return new String[]{str};

        String[] res = new String[(str.length() / n) + (str.length() % 2)];
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < str.toCharArray().length; i++) {
            sb.append(str.charAt(i));
            if (sb.length() == n) {
                res[(i + 1) / n - 1] = sb.toString();
                sb.setLength(0);
            }
        }
        if (sb.length() > 0)
            res[res.length - 1] = sb.toString();

        return res;
    }

    /**
     * Insert a character {@param insert} to string {@param str} after every n characters
     * Notice: the last character in the returned value is always the given character {@param insert}
     * if {@param n} > 0
     *
     * @param str
     * @param insert
     * @param n
     * @return
     */
    public static String insertCharEvery(String str, char insert, int n) {
        if (n <= 0)
            return str;
        String[] res = new String[str.length() + (str.length() / n)];
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < str.toCharArray().length; i += n) {
            sb.append(str, i, i + n);
            sb.append(insert);
        }
        if (str.length() % n != 0) {
            sb.append(str, str.length() - str.length() % n, str.length());
            sb.append(insert);
        }
        return sb.toString();
    }


    public static String join(String[] strArr) {
        return joinSeparated(strArr, EMPTY_STRING);
    }

    public static String joinSeparated(String[] strArr, String sep) {
        StringBuilder sb = new StringBuilder();
        for (String str : strArr)
            sb.append(str + sep);
        return sb.toString();
    }

    public static String removeChar(String str, char match) {
        StringBuilder sb = new StringBuilder();
        for (char c : str.toCharArray()) {
            if(c == match)
                continue;
            sb.append(c);
        }
        return sb.toString();
    }

    /**
     * Check if the string is null or empty
     *
     * @param string string to be checked
     * @return
     */
    public static boolean isEmpty(String string) {
        return string == null || string.equals(EMPTY_STRING);
    }

}
