package server.app.Cache;

import protocol.IMessage;
import server.app.ICrud;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Level1 cache for the Key-Value store
 */
public class CacheStorage implements ICrud {
  private int size;
  private ICacheDisplacementStrategy displacementStrategy;
  private ConcurrentHashMap<IMessage.K, IMessage> storage;

  public CacheStorage(int size) {
    this.setSize(size);
    this.setStrategy(new FIFO());
    this.storage = new ConcurrentHashMap<>(size);
  }

  @Override
  public IMessage get(IMessage.K key) {
    IMessage msg = this.storage.get(key);
    if (msg != null) {
      this.displacementStrategy.register(key);
    }

    return msg;
  }

  @Override
  public IMessage put(IMessage msg) {
    IMessage.K k = msg.getKey();
    IMessage.V v = msg.getValue();

    if (v == null) {
      this.storage.remove(k);
      return msg;
    }

    if (this.isFull()) {
      return null;
    }

    this.storage.put(k, msg);

    return msg;
  }

  /**
   * Free a space in the cache
   * @return Key-Value entry evicted from cache
   */
  public IMessage evict() {
    IMessage.K k = this.displacementStrategy.evict();
    IMessage msg = this.storage.get(k);
    if (msg != null) {
      this.storage.remove(k);
    }
    return msg;
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
