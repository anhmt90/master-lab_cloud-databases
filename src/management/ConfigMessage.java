package management;

import java.io.Serializable;

public class ConfigMessage implements Serializable {
    ConfigStatus status;
    int cacheSize;
    String strategy;
    String metadata;

    public ConfigMessage(ConfigStatus adminStatus) {
        this.status = adminStatus;
    }

    public ConfigStatus getStatus() {
        return status;
    }

    public ConfigMessage(ConfigStatus status, int cacheSize, String strategy, String metadata) {
        this.status = status;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.metadata = metadata;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getMetadata() {
        return metadata;
    }
}
