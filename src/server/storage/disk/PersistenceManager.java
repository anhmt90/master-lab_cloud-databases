package server.storage.disk;

import server.app.Server;
import server.storage.PUTStatus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.FileUtils;
import util.StringUtils;
import util.Validate;

import static util.FileUtils.SEP;
import static util.FileUtils.WORKING_DIR;

public class PersistenceManager implements IPersistenceManager {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

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
    public PUTStatus write(String key, byte[] value) {
        Path file = getFilePath(key);
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
     * constructs a directory path for a key
     * The key will be the file name and each of its characters will be a folder in the path to the file
     *
     * @param key key from which the path is constructed
     * @return a directory path corresponding to the key
     */
    public Path getFilePath(String key) {
        String path = db_path + SEP + StringUtils.insertCharEvery(key, '/', 2) + key;
        return Paths.get(path);
    }


    /**
     * Handles creating or updating a value in a given path
     *
     * @param file        path in which the value is supposed
     *                    to be stored
     * @param fileContent value being stored in a file
     * @return Status if operation was successful or failed
     */
    private PUTStatus createOrUpdate(Path file, byte[] fileContent) {
        try {
            synchronized (file) {
                if (!FileUtils.exists(file)) {
                    Files.createFile(file);
                    Files.write(file, fileContent);
                    return PUTStatus.CREATE_SUCCESS;
                }
                Files.write(file, fileContent);
                return PUTStatus.UPDATE_SUCCESS;
            }
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }
        return (FileUtils.exists(file)) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
    }

//    private PUTStatus createOrUpdate(Path file, byte[] fileContent) {
//        PUTStatus opType;
//        try {
//            try {
//                Files.createFile(file);
////            if (!FileUtils.exists(file) && !FileUtils.isBeingCreated(file.getFileName().toString())) {
////                createNewFile(file);
//                opType = PUTStatus.CREATE_SUCCESS;
////            }
//            } catch (FileAlreadyExistsException faee) {
//                opType = PUTStatus.UPDATE_SUCCESS;
//            }
//            write(file, fileContent);
//            return opType;
//        } catch (IOException e) {
//            LOG.error(e);
//            e.printStackTrace();
//        }
//        return (FileUtils.exists(file)) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
//    }

//    private void createNewFile(Path file) throws IOException {
//        boolean success = false;
//        while (!success) {
//            success = FileUtils.lockForCreating(file.getFileName().toString());
//            if (success) {
////                FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
////                fileChannel.close();
//
////                Files.createFile(file);
//            }
//        }
//        FileUtils.doneCreating(file.getFileName().toString());
//    }

//    private void write(Path file, byte[] fileContent) throws IOException {
//        while (true) {
//            try {
//                FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE);
//                fileChannel.lock();
//                fileChannel.truncate(0);
//                ByteBuffer buffer = ByteBuffer.wrap(fileContent);
//                while (buffer.hasRemaining())
//                    fileChannel.write(buffer);
//                fileChannel.force(true);
//                fileChannel.close();
//                break;
//            } catch (OverlappingFileLockException e) {
//                LOG.error("Lock error!", e);
//                try {
//                    Thread.sleep(2);
//                } catch (InterruptedException ie) {
//                    LOG.error(ie);
//                    ie.printStackTrace();
//                }
//            }
//        }
//    }

    @Override
    public byte[] read(String key) {
        Path file = getFilePath(key);
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

//    public byte[] read(String key) {
//        Path file = getFilePath(key);
//        if (FileUtils.exists(file)) {
//            try {
//                FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ);
//                fileChannel.lock(0, Long.MAX_VALUE, true);
//                ByteBuffer buffer = ByteBuffer.allocate(1024 * 120);
//                int bytesRead = fileChannel.read(buffer);
//                byte[] ret = new byte[bytesRead];
//                fileChannel.close();
//                System.arraycopy(buffer.array(), 0, ret, 0, bytesRead);
//                return ret;
//
//            } catch (IOException e) {
//                LOG.error(e);
//                e.printStackTrace();
//            }
//        }
//        return null;
//    }

    @Override
    public synchronized PUTStatus delete(String key) {
        Path file = getFilePath(key);
        if (!Files.isDirectory(file)) {
            try {
                Files.delete(file);
                return PUTStatus.DELETE_SUCCESS;
            } catch (IOException e) {
                LOG.error(e);
                e.printStackTrace();
            }
        }
        return null;
    }

//    public PUTStatus delete(String key) {
//        Path file = getFilePath(key);
//        if (!FileUtils.isDir(file)) {
//            try {
//                FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.DELETE_ON_CLOSE);
//                fileChannel.close();
//                return PUTStatus.DELETE_SUCCESS;
//            } catch (IOException e) {
//                LOG.error(e);
//                e.printStackTrace();
//            }
//        }
//        return PUTStatus.DELETE_ERROR;
//    }

    public String getDbPath() {
        return db_path;
    }
}

