package server.app;

import protocol.Message;

public class CacheManager implements ICrud {
  private CacheStorage cache;
  private PersistentStorage disk;
  private ICacheDisplacementStrategy strategy;

  private void setCache(CacheStorage cache) {
    this.cache = cache;
  }

  private void setDisk(PersistentStorage disk) {
    this.disk = disk;
  }

  private void setStrategy(ICacheDisplacementStrategy strategy) {
    this.strategy = strategy;
    this.cache.setStrategy(strategy);
  }

  @Override
  public Message get(String key) {
    Message msg = this.cache.get(key);
    if (msg == null) {
      /*
      If there is no key in cache, it can be found in persistent storage
      Synchronize to avoid overwriting new value incoming in the same time for this key
       */
      synchronized (this.cache) {
        msg = this.disk.get(key);
        if (msg != null) {
          this.put(msg.getKey(), msg.getValue());
        }
      }
    }
    return msg;
  }

  @Override
  public Message put(String key, String value) {
    Message msg;
    synchronized (this.cache) {
      if (this.cache.isFull()) {
        Message evictedMsg = this.cache.evict();
        this.disk.put(evictedMsg.getKey(), evictedMsg.getValue());
      }
      msg = this.cache.put(key, value);
    }
    return msg;
  }

  public static class Builder {
    private static String DEFAULT_DISK_PATH = "./persistent";

    private CacheStorage cache;
    private PersistentStorage disk;
    private ICacheDisplacementStrategy strategy;

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
