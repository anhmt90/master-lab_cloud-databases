package util;

import server.app.Server;

import java.io.File;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileUtils {
    public static final String SEP = "/";
    public static final String WORKING_DIR = getWorkingDir();
    public static final String USER_DIR = System.getProperty("user.dir");

    private static String getWorkingDir() {
//        String path = FileUtils.class.getClassLoader().getResource("util").getPath();
        try {
            String path =  new File(FileUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            return path.substring(0, path.lastIndexOf('/'));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


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
