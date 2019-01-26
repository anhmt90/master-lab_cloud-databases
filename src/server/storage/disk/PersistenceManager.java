package server.storage.disk;

import server.app.Server;
import server.storage.PUTStatus;

import java.awt.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.FileUtils;
import util.StringUtils;
import util.Validate;

import static util.FileUtils.SEP;
import static util.FileUtils.WORKING_DIR;

public class PersistenceManager implements IPersistenceManager {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private final ConcurrentMap<String, Object> fileLocks = new ConcurrentHashMap<>();

    private String db_path = WORKING_DIR + "/db" + SEP;

    public PersistenceManager(String serverId) {
        db_path += serverId + SEP;
        createDBDir(db_path);
    }

    /**
     * Creates a directory structure from a given path
     *
     * @param path the directory path that should be
     *             constructed
     * @return true if the creation was successful
     */
    public boolean createDBDir(String path) {
        Path dbPath = Paths.get(path);
        if (!FileUtils.dirExists(dbPath)) {
            try {
                Files.createDirectories(dbPath);
                LOG.info("New Directory Successfully Created at " + dbPath);
                return true;
            } catch (IOException ioe) {
                LOG.error("Problem occured while creating 'db' directory = " + ioe.getMessage());
            }
        }
        return false;
    }

    @Override
    public PUTStatus write(Path file, byte[] value) {
        try {
            if (!FileUtils.dirExists(file.getParent()))
                Files.createDirectories(file.getParent());
            return createOrUpdate(file, value);
        } catch (FileAlreadyExistsException faee) {
            faee.printStackTrace();
            LOG.error(faee);
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }
        return FileUtils.exists(file) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
    }




    /**
     * Handles creating or updating a value in a given path
     *
     * @param file        path in which the value is supposed
     *                    to be stored
     * @param fileContent value being stored in a file
     * @return Status if operation was successful or failed
     */
    private synchronized PUTStatus createOrUpdate(Path file, byte[] fileContent) {
        try {
            String fileName = file.getFileName().toString();
            Object lock = fileLocks.get(fileName);
            if (lock == null) {
                fileLocks.put(fileName, new Object());
                lock = fileLocks.get(fileName);
            }
            synchronized (lock) {
                if (!FileUtils.exists(file)) {
                    Files.createFile(file);
                    Files.write(file, fileContent);
                    fileLocks.remove(fileName);
                    return PUTStatus.CREATE_SUCCESS;
                }
                fileLocks.remove(fileName);
                Files.write(file, fileContent);
                return PUTStatus.UPDATE_SUCCESS;
            }
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }
        return (FileUtils.exists(file)) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
    }


    @Override
    public byte[] read(Path file) {
        if (FileUtils.exists(file)) {
            try {
                return Files.readAllBytes(file);
            } catch (IOException e) {
                LOG.error(e);
                e.printStackTrace();
            }
        }
        return null;
    }

    @Override
    public synchronized PUTStatus delete(Path file) {
        if (!Files.isDirectory(file)) {
            try {
                Files.delete(file);
                return PUTStatus.DELETE_SUCCESS;
            } catch (IOException e) {
                LOG.error(e);
            }
        }
        return null;
    }

    public String getDbPath() {
        return db_path;
    }
}

