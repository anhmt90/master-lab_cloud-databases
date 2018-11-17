package management;

import ecs.KVServerMeta;
import ecs.Metadata;
import server.storage.cache.CacheDisplacementStrategy;

import java.io.Serializable;

public class ConfigMessage implements Serializable {
    public KVServerMeta getTargetServer() {
        return targetServer;
    }

    private KVServerMeta targetServer;
    ConfigStatus status;
    int cacheSize;
    CacheDisplacementStrategy strategy;
    Metadata metadata;

    public ConfigMessage(ConfigStatus adminStatus) {
        this.status = adminStatus;
    }

    public ConfigStatus getStatus() {
        return status;
    }

    public ConfigMessage(ConfigStatus status, int cacheSize, CacheDisplacementStrategy strategy, Metadata metadata) {
        this.status = status;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.metadata = metadata;
    }

    public ConfigMessage(ConfigStatus status, KVServerMeta target) {
        this.status = status;
        this.targetServer = target;
    }

    public ConfigMessage(ConfigStatus status, Metadata md) {
      this.status = status;
      this.metadata = md;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public CacheDisplacementStrategy getStrategy() {
        return strategy;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
