package server.storage;

import protocol.IMessage;

/**
 * CRUD interface
 * Implement to comply with operations used in Key-Value store
 */
public interface ICrud {
  IMessage.V get(IMessage.K key);
  IMessage.K put(IMessage.K key, IMessage.V val);
}
