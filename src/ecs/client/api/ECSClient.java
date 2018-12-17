package ecs.client.api;

import ecs.IECS;
import server.storage.cache.CacheDisplacementStrategy;

public class ECSClient implements IECS {

	
	public ECSClient() {
		
	}

	public void initService(int numberOfNodes, int cacheSize, CacheDisplacementStrategy displacementStrategy) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) {

	}

	@Override
	public void startService() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stopService() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void shutdown() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void addNode(int cacheSize, String displacementStrategy) {

	}

	public void addNode(int cacheSize, CacheDisplacementStrategy displacementStrategy) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void removeNode() {
		// TODO Auto-generated method stub
		
	}
	
}
