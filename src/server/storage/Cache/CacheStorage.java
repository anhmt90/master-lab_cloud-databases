package server.storage.Cache;

import protocol.IMessage;
import server.storage.ICrud;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Level1 storage for the Key-Value store
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
      this.displacementStrategy.get(key);
    }

    return val;
  }

  @Override
  public IMessage.K put(IMessage.K key, IMessage.V val) {
    /**
     * remove key-value if a value equals null
     */
    if (val == null) {
      this.storage.remove(key);
      this.displacementStrategy.unregister(key);
      return key;
    }

    if (this.isFull()) {
      return null;
    }

    this.storage.put(key, val);
    this.displacementStrategy.put(key);

    return key;
  }

  /**
   * Free a space in the storage
   * @return Key-Value entry evicted from storage
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
   * Set storage displacement strategy, that decides which entry to evict from the storage
   * @param strategy displacement strategy to use
   */
  public void setStrategy(ICacheDisplacementStrategy strategy) {
    this.displacementStrategy = strategy;
  }

  public void setSize(int size) {
    this.size = size;
  }
}
