package server.app.Cache;

import protocol.IMessage;
import server.app.ICrud;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Level1 cache for the Key-Value store
 */
public class CacheStorage implements ICrud {
  private int size;
  private ICacheDisplacementStrategy displacementStrategy;
  private ConcurrentHashMap<IMessage.K, IMessage.V> storage;

  public CacheStorage(int size) {
    this.setSize(size);
    this.setStrategy(new FIFO());
    this.storage = new ConcurrentHashMap<>(size);
  }

  @Override
  public IMessage.V get(IMessage.K key) {
    IMessage.V val = this.storage.get(key);
    if (val != null) {
      this.displacementStrategy.register(key);
    }

    return val;
  }

  @Override
  public IMessage.K put(IMessage.K key, IMessage.V val) {
    if (val == null) {
      this.storage.remove(key);
      return key;
    }

    if (this.isFull()) {
      return null;
    }

    this.storage.put(key, val);

    return key;
  }

  /**
   * Free a space in the cache
   * @return Key-Value entry evicted from cache
   */
  public Map.Entry<IMessage.K, IMessage.V> evict() {
    IMessage.K k = this.displacementStrategy.evict();
    IMessage.V v = this.storage.get(k);
    if (v != null) {
      this.storage.remove(k);
    }
    return new AbstractMap.SimpleImmutableEntry<>(k, v);
  }

  public boolean isFull() {
    return this.storage.mappingCount() >= this.size;
  }

  /**
   * Set cache displacement strategy, that decides which entry to evict from the cache
   * @param strategy displacement strategy to use
   */
  public void setStrategy(ICacheDisplacementStrategy strategy) {
    this.displacementStrategy = strategy;
  }

  public void setSize(int size) {
    this.size = size;
  }
}
