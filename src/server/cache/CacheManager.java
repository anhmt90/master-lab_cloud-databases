package server.app;

import protocol.IMessage;
import server.app.Cache.*;

/** Manages available storage options by their capacities
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
  public IMessage get(IMessage.K key) {
    IMessage msg = this.cache.get(key);
    if (msg == null) {
      /*
      If there is no key in cache, it can be found in persistent storage
      Synchronize to avoid overwriting new value incoming in the same time for this key
       */
      synchronized (this.cache) {
        msg = this.disk.get(key);
        if (msg != null) {
          this.put(msg);
        }
      }
    }
    return msg;
  }

  /**
   * Stores a key-value entry in the key-value store
   * @param msg A key-value entry
   * @return A key-value entry stored
   */
  @Override
  public IMessage put(IMessage msg) {
    synchronized (this.cache) {
      if (this.cache.isFull()) {
        IMessage evictedMsg = this.cache.evict();
        this.disk.put(evictedMsg);
      }
      this.cache.put(msg);
    }
    return msg;
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
     * Sets a displacement strategy for the cache to choose entries to move to the disk
     * if the cache is full
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
