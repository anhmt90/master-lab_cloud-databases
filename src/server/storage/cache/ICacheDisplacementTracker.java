package server.storage.cache;

import protocol.K;

/**
 * cache displacement strategy, that decides which entry to evict from the storage
 */
public interface ICacheDisplacementTracker {
  K evict();

  /**
   * Register a use (get, put) of a key-value entry in storage
   * @param k
   */
  K register(K k);
  void unregister(K k);
  boolean containsKey(K key);
}
