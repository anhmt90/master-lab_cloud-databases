package ecs.client.api;

public interface IECSClient {
	
	
	void initService (int numberOfNodes, int cacheSize, String displacementStrategy);
	
	void start();
	
	void stop();
	
	void shutDown();
	
	void addNode(int cacheSize, String displacementStrategy);
	
	void removeNode();
}
