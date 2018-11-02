package server.storage;

import server.app.KVServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.regex.Pattern;

import static util.StringUtils.PATH_SEP;

public class PersistenceManager implements IPersistenceManager {
    private static final String NOT_ALPHANUMERIC = "[^A-Za-z0-9]";
    private static final String NOT_ALPHABETIC = "[^A-Za-z]";
    private static final String EMPTY = "";
    private static final String FILE_EXT = EMPTY;
    private static Pattern notAlphanumeric = Pattern.compile(NOT_ALPHANUMERIC);
    private static Pattern notAlphabetic = Pattern.compile(NOT_ALPHABETIC);

    @Override
    public OpStatus write(String key, String value) {
        Path file = getFilePath(key);
        String fileContent = format(file.getFileName().toString(), value);
        try {
            Files.createDirectories(file.getParent());
            return createOrUpdate(file, fileContent);

        } catch (FileAlreadyExistsException faee) {
            faee.printStackTrace();
            System.out.println(faee);
        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
        return OpStatus.WRITE_ERR;
    }

    private String escape(String value) {
        return value.replace("\\", "\\\\");
    }

    private Path getFilePath(String key) {
        String[] keyParts = encode(key);
        String escapedKey = String.join(EMPTY, keyParts);

        String path = KVServer.ROOT_DB_PATH
                + PATH_SEP + String.valueOf(escapedKey.length())
                + PATH_SEP + String.join(PATH_SEP, keyParts)
                + PATH_SEP + escapedKey + FILE_EXT;
        return Paths.get(path);
    }

    private String format(String key, String value) {
        return key + "=" + value;
    }

    private String[] encode(String key) {
        String[] subpaths = key.split(EMPTY);
        if (notAlphanumeric.matcher(key).find())
            for (int i = 0; i < subpaths.length; i++) {
                if (notAlphabetic.matcher(subpaths[i]).find())
                    subpaths[i] = String.valueOf(subpaths[i].getBytes(StandardCharsets.US_ASCII)[0]);
            }
        return subpaths;
    }

    private OpStatus createOrUpdate(Path file, String fileContent) {
        try {
            if(!isExisted(file))
                Files.createFile(file);
            Files.write(file, fileContent.getBytes());
            return OpStatus.WRITE_SUCCESS;
        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
        return OpStatus.WRITE_ERR;
    }


    @Override
    public String read(String key) {
        Path file = getFilePath(key);
        if (isExisted(file)) {
            try {
                List<String> lines = Files.readAllLines(file);
                for (String line : lines) {
                    return line.split("=", 2)[1];
                }
            } catch (IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public OpStatus delete(String key) {
        Path file = getFilePath(key);
        if(!Files.isDirectory(file)) {
            try {
                Files.delete(file);
                return OpStatus.DELETE_SUCCESS;
            } catch (IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        }
        return OpStatus.DELETE_ERR;
    }

    private boolean isExisted(Path filePath) {
        return !Files.isDirectory(filePath) && Files.exists(filePath);
    }
}
