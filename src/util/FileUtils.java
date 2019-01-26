package util;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;

public class FileUtils {
    public static final String SEP = "/";
    public static final String USER_DIR = System.getProperty("user.dir");
    public static final String WORKING_DIR = getWorkingDir();

    private static HashMap filesBeingCreated = new HashMap();


    private static String getWorkingDir() {
//        String path = FileUtils.class.getClassLoader().getResource("util").getPath();
        try {
            String path = new File(FileUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getPath();
            char separator = isWindows() ? '\\' : '/';
            return isWindows()? path.substring(0, path.lastIndexOf(separator)): USER_DIR;
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().indexOf("win") >= 0;
    }


    /**
     * Checks if a file path is valid
     *
     * @param filePath Path to a given file
     * @return true if the path exists
     */
    public static boolean exists(Path filePath) {
        return filePath.toFile().isFile() && filePath.toFile().exists();
    }

    /**
     * Checks if a file path is valid
     *
     * @param filePath Path to a given file
     * @return true if the path exists
     */
    public static boolean dirExists(Path filePath) {
        return filePath.toFile().isDirectory() && filePath.toFile().exists();
    }

    public static boolean isFile(Path filePath) {
        return filePath.toFile().isFile();
    }

    public static boolean isDir(Path filePath) {
        return filePath.toFile().isDirectory();
    }

    /**
     * Checks if a file name exists
     *
     * @param filePathString path of the file to be checked as string
     * @return true if file exists
     */
    public static boolean exists(String filePathString) {
        return exists(Paths.get(filePathString));
    }


    public static boolean deleteIfExists(Path filePath) {
        if (exists(filePath))
            return filePath.toFile().delete();
        return false;
    }

    public synchronized static boolean lockForCreating(String fileName) {
        if (!filesBeingCreated.containsKey(fileName)) {
            filesBeingCreated.put(fileName, fileName);
            return true;
        }
        return false;
    }

    public synchronized static void doneCreating(String fileName) {
        filesBeingCreated.remove(fileName);
    }

    public static boolean isBeingCreated(String fileName) {
        return filesBeingCreated.containsKey(fileName);
    }

    public static byte[] getValueBytes(String file) throws IOException {
        return Files.readAllBytes(Paths.get(file));
    }

    public static String getValue(String file) throws IOException {
        return new String(Files.readAllBytes(Paths.get(file)));
    }
}
