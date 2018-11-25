package util;

import org.apache.logging.log4j.Logger;

public class LogUtils {

    public static boolean exitWithError(Logger LOG, Exception e){
        LOG.error(e);
        return false;
    }

    /**
     * Logging of errors
     *
     * @param exception the exception that was thrown in the error
     * @param message   the error message
     * @return exception
     */
    public static <E> E printLogError(Logger LOG, E exception, String message) {
        System.out.print(message + "\n");
        LOG.error(message, exception);
        return exception;
    }
}
