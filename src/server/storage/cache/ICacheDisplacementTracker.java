package server.storage.cache;

import protocol.K;

/**
 * cache displacement strategy, that decides which entry to evict from the storage
 */
public interface ICacheDisplacementTracker {
    /**
     * Chooses a key to remove from the cache to free up the space according to a chosen strategy
     * @return evicted key
     */
    K evict();

    /**
     * Register a use (get, put) of a key-value entry in storage
     *
     * @param key
     */
    K register(K key);

    /**
     * Stop tracking a use of key {@link K}
     * @param k key of tuple that should be omitted out of cache
     */
    void unregister(K k);

    boolean containsKey(K key);
}
