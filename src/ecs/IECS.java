package ecs;

import server.storage.cache.CacheDisplacementStrategy;

import java.io.IOException;

/**
 * Interface the External Configuration Service provides to the ECS Client
 *
 */
public interface IECS {
	
	/**
	 * Starts up the storage service on a number of servers and establishes connection with them
	 * 
	 * @param numberOfNodes the number of servers that are started up
	 * @param cacheSize the cache loadedDataSize the servers are started with
	 * @param displacementStrategy the strategy according to which the servers manage their cache
	 * @throws Exception
	 */
	void initService (int numberOfNodes, int cacheSize, String displacementStrategy) throws Exception;
	
	/**
	 * Starts the storage service on all connected servers
	 */
	void startService();
	
	/**
	 * Stops the storage service on all connected servers
	 */
	void stopService();
	
	/**
	 * Shutsdown all connected servers and releases the connection
	 */
	void shutDown();
	
	/**
	 * Adds a storage server to the storage service
	 * 
	 * @param cacheSize the cache loadedDataSize the server is started with
	 * @param displacementStrategy the strategy according to which the server manages its cache
	 * @throws InterruptedException
	 * @throws IOException
	 */
	void addNode(int cacheSize, String displacementStrategy) throws InterruptedException, IOException;
	
	/**
	 * Removes an arbitrary node from the storage service
	 */
	void removeNode();
}
