package server.storage.disk;

import server.api.ClientConnection;
import server.app.Server;
import server.storage.PUTStatus;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.FileUtils;
import util.StringUtils;

import static util.FileUtils.SEP;

public class PersistenceManager implements IPersistenceManager {
	private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
	
	private String db_path = System.getProperty("user.dir") + SEP + "db" + SEP;

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
        if (!Files.exists(dbPath)) {
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
    public synchronized PUTStatus write(String key, byte[] value) {
        Path file = getFilePath(key);
        PUTStatus putStatus = FileUtils.isExisted(file) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
        try {
            Files.createDirectories(file.getParent());
            putStatus = createOrUpdate(file, value);
        } catch (FileAlreadyExistsException faee) {
            faee.printStackTrace();
            LOG.error(faee);
        } catch (IOException e) {
            LOG.error(e);
            e.printStackTrace();
        }
        return putStatus;
    }

    /**
     * constructs a directory path for a key
     * The key will be the file name and each of its characters will be a folder in the path to the file
     *
     * @param key key from which the path is constructed
     * @return a directory path corresponding to the key
     */
	public Path getFilePath(String key) {
		String path = db_path  + SEP + StringUtils.insertCharEvery(key, '/',2) + key;
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
	private synchronized PUTStatus createOrUpdate(Path file, byte[] fileContent) {
		try {
			if (!FileUtils.isExisted(file)) {
				Files.createFile(file);
				Files.write(file, fileContent);
				return PUTStatus.CREATE_SUCCESS;
			}
			Files.write(file, fileContent);
			return PUTStatus.UPDATE_SUCCESS;
		} catch (IOException e) {
			LOG.error(e);
			e.printStackTrace();
		}
		return (FileUtils.isExisted(file)) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
	}

    @Override
    public byte[] read(String key) {
        Path file = getFilePath(key);
        if (FileUtils.isExisted(file)) {
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
    public PUTStatus delete(String key) {
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

    public String getDbPath() {
        return db_path;
    }
}

