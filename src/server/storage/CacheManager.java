package server.storage;

import protocol.K;
import protocol.V;
import server.storage.cache.*;
import server.storage.disk.PersistenceManager;
import util.Validate;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages available pm options by their capacities
 */
public class CacheManager implements IStorageCRUD {
    public static final String ERROR = "ERROR";
    private ICacheDisplacementTracker cacheTracker;
    private ConcurrentHashMap<K, V> cache;
    private int cacheCapacity;

    private PersistenceManager pm;

    public CacheManager(int cacheCapacity, CacheDisplacementStrategy strategy) {
        this.cacheCapacity = cacheCapacity;
        this.cache = new ConcurrentHashMap<>(cacheCapacity, 1);
        pm = new PersistenceManager();
        cacheTracker = initTracker(strategy);
    }

    private ICacheDisplacementTracker initTracker(CacheDisplacementStrategy strategy) {
        switch (strategy) {
            case FIFO:
                return new FIFO();
            case LFU:
                return new LFU();
            case LRU:
                return new LRU();
            default:
                throw new IllegalArgumentException("Strategy not found!");
        }
    }


    /**
     * Get a message from Key-Value cache
     *
     * @param key Key of a ke-value entry
     * @return Key-value entry as a message
     */
    @Override
    public V get(K key) {
        byte[] res = pm.read(key.get());
        return (res != null) ? new V(res) : null;
    }

    /**
     * Stores a key-value entry in the key-value cache
     *
     * @param key key of a new entry
     * @param val value of a new entry
     * @return A key-value entry stored
     */
    @Override
    public PUTStatus put(K key, V val) {
        PUTStatus status = (val != null) ? pm.write(key.get(), val.get()) : pm.delete(key.get());
        if (status.name().contains(ERROR))
            return status;
        updateCache(key, val);
        return status;
    }

    private void updateCache(K key, V val) {
        if (val != null) {
            updateCacheForWriteOp(key, val);
        } else if (val == null && cache.containsKey(key)) {
            updateCacheForDeleteOp(key);
        }
    }

    private void updateCacheForDeleteOp(K key) {
        Validate.isTrue(cacheTracker.containsKey(key), "cache and its tracker are out of sync");
        cache.remove(key);
        K evicted = cacheTracker.evict();
        Validate.isTrue(key.equals(evicted), "Wrong key evicted");
    }

    private void updateCacheForWriteOp(K key, V val) {
        if (isCacheFull()) {
            K evicted = cacheTracker.evict();
            Validate.isTrue(cache.containsKey(evicted), "cache and its tracker are out of sync");
            cache.remove(evicted);
        }
        cache.put(key, val);
        cacheTracker.register(key);
    }


    public boolean isCacheFull() {
        return cache.mappingCount() >= this.cacheCapacity;
    }

}
