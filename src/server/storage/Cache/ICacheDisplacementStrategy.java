package server.storage.Cache;

import protocol.IMessage;

/**
 * Cache displacement strategy, that decides which entry to evict from the storage
 */
public interface ICacheDisplacementStrategy {
  IMessage.K evict();

  /**
   * Register a use (get, put) of a key-value entry in storage
   * @param k
   */
  void register(IMessage.K k);
  void unregister(IMessage.K k);
  void put(IMessage.K k);
  void get(IMessage.K k);
}
