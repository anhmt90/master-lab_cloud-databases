package ecs;

import server.storage.cache.CacheDisplacementStrategy;

public interface IECS {
	
	
	void initService (int numberOfNodes, int cacheSize, CacheDisplacementStrategy displacementStrategy);
	
	void startService();
	
	void stopService();
	
	void shutDown();
	
	void addNode(int cacheSize, CacheDisplacementStrategy displacementStrategy);
	
	void removeNode();
}
