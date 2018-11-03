package server.storage.Cache;

import protocol.IMessage;
import protocol.K;

/**
 * Cache displacement strategy, that decides which entry to evict from the storage
 */
public interface ICacheDisplacementStrategy {
  K evict();

  /**
   * Register a use (get, put) of a key-value entry in storage
   * @param k
   */
  void register(K k);
  void unregister(K k);
  void put(K k);
  void get(K k);
}
