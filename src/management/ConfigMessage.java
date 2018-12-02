package management;

import ecs.KeyHashRange;
import ecs.NodeInfo;
import ecs.Metadata;

import java.io.Serializable;

public class ConfigMessage implements Serializable {
    private ConfigStatus status;
    private int cacheSize;
    private String strategy;
    private Metadata metadata;
    private NodeInfo targetServer;
    private KeyHashRange keyRange;

    public ConfigMessage(ConfigStatus status) {
        this.status = status;
    }

    public ConfigStatus getStatus() {
        return status;
    }

    public ConfigMessage(ConfigStatus status, int cacheSize, String strategy, Metadata metadata) {
        this.status = status;
        this.cacheSize = cacheSize;
        this.strategy = strategy;
        this.metadata = metadata;
    }

    public ConfigMessage(ConfigStatus status, NodeInfo target) {
        this.status = status;
        this.targetServer = target;
    }

    public ConfigMessage(ConfigStatus status, KeyHashRange keyRange, NodeInfo target) {
        this.status = status;
        this.keyRange = keyRange;
        this.targetServer = target;
    }

    public ConfigMessage(ConfigStatus status, Metadata md) {
        this.status = status;
        this.metadata = md;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public String getStrategy() {
        return strategy;
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public NodeInfo getTargetServer() {
        return targetServer;
    }

    public KeyHashRange getKeyRange() {
        return keyRange;
    }

    @Override
    public String toString() {
        return "ConfigMessage{" +"status=" + status +'}';
    }
}
