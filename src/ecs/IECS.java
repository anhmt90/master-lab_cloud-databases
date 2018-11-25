package ecs;

import server.storage.cache.CacheDisplacementStrategy;

public interface IECS {
	
	
	void initService (int numberOfNodes, int cacheSize, String displacementStrategy) throws Exception;
	
	void startService();
	
	void stopService();
	
	void shutDown();
	
	void addNode(int cacheSize, String displacementStrategy);
	
	void removeNode();
}
