package server.storage.disk;

import server.storage.PUTStatus;

import java.nio.file.Path;

/**
 * Persistence Manager
 * Manages data on disk 
 */
public interface IPersistenceManager {

	/**
	 * Writes a value to file
	 * 
	 * @param file   determines the path the file is stored
	 * @param value the value that is stored
	 * @return Status indicating if operation was successful
	 */
    PUTStatus write(Path file, byte[] value);

    /**
     * Reads a value from a file
     * 
     * @param file path to the file that is supposed to be read from
     * @return the value of the file
     */
    byte[] read(Path file);

    /**
     * Delete a file
     *
     * @param file path to the file that will be deleted
     * @return Status indicating if operation was successful
     */
    PUTStatus delete(Path file);
}