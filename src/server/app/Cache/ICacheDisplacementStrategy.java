package server.app.Cache;

import protocol.IMessage;

/**
 * Cache displacement strategy, that decides which entry to evict from the cache
 */
public interface ICacheDisplacementStrategy {
  IMessage.K evict();

  /**
   * Register a use (get, put) of a key-value entry in cache
   * @param key
   */
  void register(IMessage.K key);
}
