package util;

public class StringUtils {
    public static final String PATH_SEP = "/";
    private static final char LINE_FEED = 0x0A;
    private static final char RETURN = 0x0D;
    private static final String EMPTY_STRING = "";


    public static byte[] toByteArray(String s){
        byte[] bytes = s.getBytes();
        byte[] ctrBytes = new byte[]{LINE_FEED, RETURN};
        byte[] tmp = new byte[bytes.length + ctrBytes.length];

        System.arraycopy(bytes, 0, tmp, 0, bytes.length);
        System.arraycopy(ctrBytes, 0, tmp, bytes.length, ctrBytes.length);

        return tmp;
    }

    public static boolean isEmpty (String s){
        return s == null || s.equals(EMPTY_STRING);
    }
}
