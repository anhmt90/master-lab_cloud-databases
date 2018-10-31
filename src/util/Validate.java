package util;

public class Validate {

    public static void isTrue(final boolean expression, final String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public static <T> void notNull(final T toCheck, String message) {
        if (toCheck == null)
            throw new NullPointerException(message);
    }
}
