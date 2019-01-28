package server.storage.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.K;
import protocol.kv.V;
import server.app.Server;
import server.storage.IStorageCRUD;
import server.storage.PUTStatus;
import server.storage.disk.PersistenceManager;
import util.FileUtils;
import util.StringUtils;
import util.Validate;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;

import static protocol.mapreduce.Utils.NODEID_KEYBYTES_SEP;

/**
 * Manages the cache and plays as an coordinator between {@link server.api.ClientConnection} and {@link PersistenceManager}.
 * This class handles client's put and get requests by maintaning a {@link this.cache} for quick access. In case the requested key
 * does not reside in the cache, {@link CacheManager} will forward the request to {@link PersistenceManager} to lookup the key
 * in persistence layer e.g. file system.
 * If the {@link this.cache} reach its {@link this.cacheCapacity}, {@link CacheManager} will replace a <{@link K}, {@link V}> pair in {@link this.cache}
 * by the pair having the currently requested key according to the current {@link CacheDisplacementStrategy}, which is implemented
 * by the {@link this.cacheTracker}
 */
public class CacheManager implements IStorageCRUD {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

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

    public CacheManager(String serverName, int cacheCapacity, CacheDisplacementStrategy strategy) {
        this.cacheCapacity = cacheCapacity;
        this.cache = new ConcurrentHashMap<K, V>(cacheCapacity + 1, 1);
        pm = new PersistenceManager(serverName);
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
    public V get(K key, String MRToken) {
        V val;
        if (StringUtils.isEmpty(MRToken) && cache.containsKey(key)) {
            val = cache.get(key);
            updateCache(key, val);
//            new Thread(new CacheUpdater(this, key, val)).start();
            return val;
        }

        Path filePath = buildPath(key.getHashed(), buildPUTFileName(MRToken, key));

        byte[] res = pm.read(filePath);
        if (res == null)
            return null;
        val = new V(new String(res));
        updateCache(key, val);
//        new Thread(new CacheUpdater(this, key, val)).start();
        return val;
    }

    /**
     * Stores or deletes a <K,V> pair in file system by calling {@link PersistenceManager} according to response to the client request.
     * After a successful update in file system, the <K,V> is brought to cache and a cache displacement can happen if the cache reaches
     * its {@link this.cacheCapacity}.
     *
     * @param key key in <K,V> pair. Being used to search for the relevant pair in file system
     * @param val key in <K,V> pair. The value that should be stored or deleted on the server.r
     * @return {@link PUTStatus} as exit code of the function. This will be used to send an appropriate response back to the client.
     */
    @Override
    public PUTStatus put(K key, V val, String MRToken) {
        Path filePath = buildPath(key.getHashed(), buildPUTFileName(MRToken, key));

        PUTStatus status = (val != null) ? pm.write(filePath, val.getBytes()) : pm.delete(filePath);
        if (status.name().contains(ERROR))
            return status;
        updateCache(key, val);
//        new Thread(new CacheUpdater(this, key, val)).start();
        return status;
    }


    private String buildPUTFileName(String MRToken, K key) {
        String fileName = StringUtils.isEmpty(MRToken) ? StringUtils.EMPTY_STRING : MRToken + NODEID_KEYBYTES_SEP;
        fileName += key.getByteString();
        return fileName;
    }

    private String buildGETFileName(String MRToken, K key) {
        String fileName = StringUtils.isEmpty(MRToken) ? StringUtils.EMPTY_STRING : MRToken + NODEID_KEYBYTES_SEP;
        fileName += key.getByteString();
        return fileName;
    }

    private Path buildPath(String keyHashed, String fileName) {
        Path filePath = FileUtils.buildPath(pm.getDbPath(), keyHashed, fileName);
        LOG.info(filePath.toString() + " constructed" );
        return filePath;
    }


    /**
     * Updates the {@link this.cache} with the given <K,V> pair. A cache displacement can happen if the cache reaches
     * its {@link this.cacheCapacity}.
     *
     * @param key key in <K,V> pair. Being used to search for the relevant pair in {@link this.cache}
     * @param val key in <K,V> pair. The value that should be stored/updated on the {@link this.cache}.
     */
    synchronized void updateCache(K key, V val) {
        if (val != null) {
            updateCacheForReadWriteOp(key, val);
        } else if (val == null && cache.containsKey(key)) {
            updateCacheForDeleteOp(key);
        }
    }

    private void updateCacheForDeleteOp(K key) {
        Validate.isTrue(cacheTracker.containsKey(key), key.getHashed() + " is not in cache. Cache and its tracker are out of sync");
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
