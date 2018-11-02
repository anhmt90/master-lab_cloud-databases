package server.storage.disk;

import protocol.IMessage;
import server.app.KVServer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    public OpStatus write(IMessage.K key, IMessage.V value) {
        Path file = getFilePath(key);
        String fileContent = escape(value.toString());
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

    public Path getFilePath(IMessage.K k) {
        String[] keyParts = encode(k);
        String escapedKey = String.join(EMPTY, keyParts);

        String path = KVServer.ROOT_DB_PATH
                + PATH_SEP + String.join(PATH_SEP, keyParts)
                + PATH_SEP + escapedKey + FILE_EXT;
        return Paths.get(path);
    }

    private String[] encode(IMessage.K k) {
        String key = k.toString();
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
    public String read(IMessage.K k) {
        Path file = getFilePath(k);
        if (isExisted(file)) {
            try {
                List<String> lines = Files.readAllLines(file);
                String ret = EMPTY;
                for (String line : lines) {
                    ret += line;
                }
                return ret;
            } catch (IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public OpStatus delete(IMessage.K key) {
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
