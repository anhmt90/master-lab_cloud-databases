package server.storage.cache;

public enum CacheDisplacementStrategy {
    FIFO("FIFO"),
    LRU("LRU"),
    LFU("LFU");

    private String desc;
    CacheDisplacementStrategy(String desc) {
        this.desc = desc;
    }

    public String getDesc() {
        return desc;
    }
}
