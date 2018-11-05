package util;

public class StringUtils {
    public static final String PATH_SEP = "/";
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static final String EMPTY_STRING = "";


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
