package server.storage.disk;

import server.storage.PUTStatus;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static util.StringUtils.PATH_SEP;

public class PersistenceManager implements IPersistenceManager {
    private static final String DB_PATH = System.getProperty("user.dir") + PATH_SEP + "db" + PATH_SEP;

    public PersistenceManager() {
        createDBDir(DB_PATH);
    }

    public boolean createDBDir(String path) {
        Path dbPath = Paths.get(path);
        if (!Files.exists(dbPath)) {
            try {
                Files.createDirectories(dbPath);
                System.out.println("New Directory Successfully Created !"); //TODO change to LOG
                return true;
            } catch (IOException ioe) {
                System.out.println("Problem occured while creating 'db' directory = " + ioe.getMessage()); //TODO change to LOG
            }
        }
        return false;
    }

    @Override
    public PUTStatus write(byte[] key, byte[] value) {
        Path file = getFilePath(key);
        PUTStatus putStatus = isExisted(file) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
        try {
            Files.createDirectories(file.getParent());
            putStatus = createOrUpdate(file, value);
        } catch (FileAlreadyExistsException faee) {
            faee.printStackTrace();
            System.out.println(faee);
        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
        return putStatus;
    }

    public Path getFilePath(byte[] key) {
        String path = DB_PATH + formatFilePath(key) + formatFileName(key);
        return Paths.get(path);
    }

    private String formatFilePath(byte[] key) {
        return formatFileName(key).replaceAll("..", "$0/");
    }

    private String formatFileName(byte[] key) {
        return Arrays.toString(key).replaceAll("\\W", "");
    }

    private PUTStatus createOrUpdate(Path file, byte[] fileContent) {
        try {
            if (!isExisted(file)) {
                Files.createFile(file);
                Files.write(file, fileContent);
                return PUTStatus.CREATE_SUCCESS;
            }
            Files.write(file, fileContent);
            return PUTStatus.UPDATE_SUCCESS;
        } catch (IOException e) {
            System.out.println(e);
            e.printStackTrace();
        }
        return null;
    }


    @Override
    public byte[] read(byte[] key) {
        Path file = getFilePath(key);
        if (isExisted(file)) {
            try {
                return Files.readAllBytes(file);
            } catch (IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public PUTStatus delete(byte[] key) {
        Path file = getFilePath(key);
        if (!Files.isDirectory(file)) {
            try {
                Files.delete(file);
                return PUTStatus.DELETE_SUCCESS;
            } catch (IOException e) {
                System.out.println(e);
                e.printStackTrace();
            }
        }
        return null;
    }

    private boolean isExisted(Path filePath) {
        return !Files.isDirectory(filePath) && Files.exists(filePath);
    }
}
