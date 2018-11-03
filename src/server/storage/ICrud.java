package server.storage;

import protocol.K;
import protocol.V;

/**
 * CRUD interface
 * Implement to comply with operations used in Key-Value store
 */
public interface ICrud {
  V get(K key);
  K put(K key, V val);
}
