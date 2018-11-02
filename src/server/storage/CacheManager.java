package server.storage;

import protocol.IMessage;
import server.storage.Cache.*;

import java.util.Map;

/** Manages available disk options by their capacities
 */
public class CacheManager implements ICrud {
  private CacheStorage cache;
  private PersistentStorage disk;

  private void setCache(CacheStorage cache) {
    this.cache = cache;
  }

  private void setDisk(PersistentStorage disk) {
    this.disk = disk;
  }

  private void setStrategy(ICacheDisplacementStrategy strategy) {
    this.cache.setStrategy(strategy);
  }

  /**
   * Get a message from Key-Value store
   * @param key Key of a ke-value entry
   * @return Key-value entry as a message
   */
  @Override
  public IMessage.V get(IMessage.K key) {
    IMessage.V val = this.cache.get(key);
    if (val == null) {
      /*
      If there is no key in storage, it can be found in persistent disk
      Synchronize to avoid overwriting new value incoming in the same time for this key
       */
      synchronized (this.cache) {
        val = this.disk.get(key);
        if (val != null) {
          this.put(key, val);
        }
      }
    }
    return val;
  }

  /**
   * Stores a key-value entry in the key-value store
   * @param key A key-value entry
   * @param val
   * @return A key-value entry stored
   */
  @Override
  public IMessage.K put(IMessage.K key, IMessage.V val) {
    synchronized (this.cache) {
      if (this.cache.isFull()) {
        Map.Entry<IMessage.K, IMessage.V> evictedEntry = this.cache.evict();
        this.disk.put(evictedEntry.getKey(), evictedEntry.getValue());
      }
      this.cache.put(key, val);
    }
    return key;
  }

  /**
   * Builder for the {@link CacheManager}
   */
  public static class Builder {
    private static String DEFAULT_DISK_PATH = "./persistent";

    private CacheStorage cache;
    private PersistentStorage disk;
    private ICacheDisplacementStrategy strategy;

    /**
     * {@link CacheManager} builder constructor that sets default parameters,
     * use {@link Builder} setters to change the {@link CacheManager} parameters,
     * then call {@link #build()} to get a new {@link CacheManager} object
     */
    public Builder() {
      this.setCacheSize(100);
      this.setDiskStoragePath(DEFAULT_DISK_PATH);
      this.setStrategy(CacheDisplacementType.FIFO);
    }

    public Builder setCacheSize(int size) {
      this.cache = new CacheStorage(size);
      return this;
    }

    public Builder setDiskStoragePath(String path) {
      this.disk = new PersistentStorage(path);
      return this;
    }

    /**
     * Sets a displacement strategy for the storage to choose entries to move to the disk
     * if the storage is full
     * @param strategyType displacement strategy type
     * @return {@link Builder} for the {@link CacheManager}
     */
    public Builder setStrategy(CacheDisplacementType strategyType) {
      switch(strategyType) {
        case LFU:
          this.strategy = new LFU();
          break;
        case LRU:
          this.strategy = new LRU();
          break;
        case FIFO:
          this.strategy = new FIFO();
          break;
      }

      return this;
    }

    public CacheManager build() {
      return new CacheManager(this);
    }
  }

  private CacheManager(Builder builder) {
    this.setCache(builder.cache);
    this.setDisk(builder.disk);
    this.setStrategy(builder.strategy);
  }
}
