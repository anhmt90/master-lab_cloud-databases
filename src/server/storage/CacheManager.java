package server.storage;

import protocol.K;
import protocol.V;
import server.storage.cache.*;
import server.storage.disk.PersistenceManager;
import util.Validate;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages the cache and plays as an coordinator between {@link server.api.ClientConnection} and {@link PersistenceManager}.
 * This class handles client's put and get requests by maintaning a {@link this.cache} for quick access. In case the requested key
 * does not reside in the cache, {@link CacheManager} will forward the request to {@link PersistenceManager} to lookup the key
 * in persistence layer e.g. file system.
 * If the {@link this.cache} reach its {@link this.cacheCapacity}, {@link CacheManager} will replace a <{@link K}, {@link V}> pair in {@link cache}
 * by the pair having the currently requested key according to the current {@link CacheDisplacementStrategy}, which is implemented
 * by the {@link this.cacheTracker}
 */
public class CacheManager implements IStorageCRUD {
    public static final String ERROR = "ERROR";
    /**
     * Keeps track of the order in which the elements in {@link this.cache} should be replaced
     */
    private ICacheDisplacementTracker cacheTracker;

    /**
     * Store <K,V> pairs for quick access. It helps avoid disk I/O for each request.
     */
    private ConcurrentHashMap<K, V> cache;

    /**
     * Maximum number of elements the {@link this.cache} can hold
     */
    private int cacheCapacity;

    /**
     * Interface to persistence layer
     */
    private PersistenceManager pm;

    public CacheManager(int cacheCapacity, CacheDisplacementStrategy strategy) {
        this.cacheCapacity = cacheCapacity;
        this.cache = new ConcurrentHashMap<>(cacheCapacity + 1, 1);
        pm = new PersistenceManager();
        cacheTracker = initTracker(cacheCapacity, strategy);
    }

    /**
     * Initializes an approriate instance of {@link ICacheDisplacementTracker} according to the provided
     * {@link CacheDisplacementStrategy}
     *
     * @param cacheCapacity Maximum number of elements the {@link this.cacheTracker} can hold
     * @param strategy
     * @return
     */
    private ICacheDisplacementTracker initTracker(int cacheCapacity, CacheDisplacementStrategy strategy) {
        switch (strategy) {
            case FIFO:
                return new FIFO(cacheCapacity);
            case LFU:
                return new LFU(cacheCapacity);
            case LRU:
                return new LRU(cacheCapacity);
            default:
                throw new IllegalArgumentException("Strategy not found!");
        }
    }


    /**
     * Get a <K,V> pair in cache to response to the client request. If the provided parameter {@link this.key} is not found
     * in cache, the request is forwarded to {@link PersistenceManager} to lookup in file system.
     *
     * @param key is used to search for the relevant pair in file system or in cache
     * @return a value associated with the {@link this.key}.
     */
    @Override
    public V get(K key) {
        V val;
        if (cache.containsKey(key)) {
            val = cache.get(key);
            updateCache(key, val);
            return val;
        }
        byte[] res = pm.read(key.get());
        val = new V(res);
        updateCache(key, val);
        return (res != null) ? new V(res) : null;
    }

    /**
     * Stores or deletes a <K,V> pair in file system by calling {@link PersistenceManager} according to response to the client request.
     * After a successful update in file system, the <K,V> is brought to cache and a cache displacement can happen if the cache reaches
     * its {@link this.cacheCapacity}.
     *
     * @param key key in <K,V> pair. Being used to search for the relevant pair in file system
     * @param val key in <K,V> pair. The value that should be stored or deleted on the server.
     * @return {@link PUTStatus} as exit code of the function. This will be used to send an appropriate response back to the client.
     */
    @Override
    public PUTStatus put(K key, V val) {
        PUTStatus status = (val != null) ? pm.write(key.get(), val.get()) : pm.delete(key.get());
        if (status.name().contains(ERROR))
            return status;
        updateCache(key, val);
        return status;
    }

    /**
     *  Updates the {@link this.cache} with the given <K,V> pair. A cache displacement can happen if the cache reaches
     *  its {@link this.cacheCapacity}.
     *
     * @param key key in <K,V> pair. Being used to search for the relevant pair in {@link this.cache}
     * @param val key in <K,V> pair. The value that should be stored/updated on the {@link this.cache}.
     */
    private void updateCache(K key, V val) {
        if (val != null) {
            updateCacheForReadWriteOp(key, val);
        } else if (val == null && cache.containsKey(key)) {
            updateCacheForDeleteOp(key);
        }
    }

    private void updateCacheForDeleteOp(K key) {
        Validate.isTrue(cacheTracker.containsKey(key), key.getString() + " is not in cache. Cache and its tracker are out of sync");
        cache.remove(key);
        cacheTracker.unregister(key);
    }

    private void updateCacheForReadWriteOp(K key, V val) {
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

    public ConcurrentHashMap<K, V> getCache() {
        return cache;
    }

    public PersistenceManager getPersistenceManager() {
        return pm;
    }


    public int getCacheCapacity() {
        return cacheCapacity;
    }

    public ICacheDisplacementTracker getCacheTracker() {
        return cacheTracker;
    }
}
