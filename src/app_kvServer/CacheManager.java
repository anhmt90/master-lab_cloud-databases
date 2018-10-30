package app_kvServer;

import static app_kvServer.CacheDisplacementType.FIFO;
import static app_kvServer.CacheDisplacementType.LFU;
import static app_kvServer.CacheDisplacementType.LRU;

public class CacheManager {
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
  }

  public static class Builder {
    private CacheStorage cache;
    private PersistentStorage disk;
    private ICacheDisplacementStrategy strategy;

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
  }

  private CacheManager(Builder builder) {
    this.setCache(builder.cache);
    this.setDisk(builder.disk);
    this.setStrategy(builder.strategy);
  }
}
