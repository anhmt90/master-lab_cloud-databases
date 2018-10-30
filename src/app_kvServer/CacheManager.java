package app_kvServer;

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
