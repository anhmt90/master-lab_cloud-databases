package server.app;

import protocol.Message;

public class CacheStorage implements ICrud {
  public CacheStorage(int size) {
  }

  @Override
  public Message get(String key) {
    return null;
  }

  @Override
  public Message put(String key, String value) {
    return null;
  }

  public Message evict() {
    return null;
  }

  public boolean isFull() {
    return false;
  }

  public void setStrategy(ICacheDisplacementStrategy strategy) {
  }
}
