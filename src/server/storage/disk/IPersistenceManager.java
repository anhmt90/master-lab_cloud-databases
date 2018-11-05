package server.storage.disk;

import server.storage.PUTStatus;

/**
 * Persistence Manager
 * Manages data on disk 
 */
public interface IPersistenceManager {

	/**
	 * Writes a value to file
	 * 
	 * @param key   determines the path the file is stored
	 * @param value the value that is stored
	 * @return Status indicating if operation was successful
	 */
    PUTStatus write(byte[] key, byte[] value);

    /**
     * Reads a value from a file
     * 
     * @param key path to the file that is supposed to be read from
     * @return the value of the file
     */
    byte[] read(byte[] key);

    /**
     * Delete a file
     * 
     * @param key path to the file that will be deleted
     * @return Status indicating if operation was successful
     */
    PUTStatus delete(byte[] key);

}
