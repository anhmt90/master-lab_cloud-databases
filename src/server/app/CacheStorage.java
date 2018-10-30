package app_kvServer;

import common.messages.KVMessage;

public class CacheStorage implements ICrud {
  public CacheStorage(int size) {
  }

  @Override
  public KVMessage get(String key) {
    return null;
  }

  @Override
  public KVMessage put(String key, String value) {
    return null;
  }

  public KVMessage evict() {
    return null;
  }

  public boolean isFull() {
    return false;
  }

  public void setStrategy(ICacheDisplacementStrategy strategy) {
  }
}
