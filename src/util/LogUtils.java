package util;

import org.apache.logging.log4j.Logger;

public class LogUtils {

    public static boolean exitWithError(Logger LOG, Exception e){
        LOG.error(e);
        return false;
    }
}
