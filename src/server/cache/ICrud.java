package server.app;

import protocol.IMessage;

/**
 * CRUD interface
 * Implement to comply with operations used in Key-Value store
 */
public interface ICrud {
  IMessage get(IMessage.K key);
  IMessage put(IMessage msg);
}
