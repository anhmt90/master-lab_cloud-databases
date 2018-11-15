package util;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    public static final String WORKING_DIR = System.getProperty("user.dir");
    public static final String SEP = "/";
    /**
     * Checks if a file path is valid
     *
     * @param filePath Path to a given file
     * @return true if the path exists
     */
    public static boolean isExisted(Path filePath) {
        return !Files.isDirectory(filePath) && Files.exists(filePath);
    }

    /**
     * Checks if a file name exists
     *
     * @param filePathString path of the file to be checked as string
     * @return true if file exists
     */
    public static boolean isExisted(String filePathString) {
        Path filePath = Paths.get(filePathString);
        return isExisted(filePath);
    }
}
