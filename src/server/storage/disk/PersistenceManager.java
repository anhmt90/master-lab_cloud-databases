package server.storage.disk;

import server.api.ClientConnection;
import server.app.Server;
import server.storage.PUTStatus;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static util.StringUtils.PATH_SEP;

public class PersistenceManager implements IPersistenceManager {
	private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
	
	private static final String DB_PATH = System.getProperty("user.dir") + PATH_SEP + "db" + PATH_SEP;

    public PersistenceManager() {
        createDBDir(DB_PATH);
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
    public PUTStatus write(byte[] key, byte[] value) {
        Path file = getFilePath(key);
        PUTStatus putStatus = isExisted(file) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
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
     * 
     * @param key key from which the path is constructed
     * @return a directory path corresponding to the key
     */
	public Path getFilePath(byte[] key) {
		String path = DB_PATH + PATH_SEP + parseFilePath(key) + PATH_SEP + parseFileName(key);
		return Paths.get(path);
	}

	/**
	 * Parses a file path from a key
	 * 
	 * @param key key from which path is parsed
	 * @return file path in String format
	 */
    private String parseFilePath(byte[] key) {
        return Arrays.toString(key).replaceAll("[\\[ \\]]", "")
                .replaceAll(",", "/");
    }

    /**
     * Parses a file name from a key
     * 
     * @param key key from which file name is parsed
     * @return file name in String format
     */
    private String parseFileName(byte[] key) {
        return Arrays.toString(key).replaceAll("\\W", "");
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
			if (!isExisted(file)) {
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
		return (isExisted(file)) ? PUTStatus.UPDATE_ERROR : PUTStatus.CREATE_ERROR;
	}

    @Override
    public byte[] read(byte[] key) {
        Path file = getFilePath(key);
        if (isExisted(file)) {
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
    public PUTStatus delete(byte[] key) {
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

    /**
     * Checks if a file path is valid
     * 
     * @param filePath Path to a given file
     * @return true if the path exists
     */
    private boolean isExisted(Path filePath) {
        return !Files.isDirectory(filePath) && Files.exists(filePath);
    }

    /**
     * Checks if a file name exists
     * 
     * @param fileName name of the file that is checked
     * @return true if file exists
     */
    public boolean isExisted(String fileName) {
        Path filePath = getFilePath(fileName.getBytes());
        return !Files.isDirectory(filePath) && Files.exists(filePath);
    }
}

