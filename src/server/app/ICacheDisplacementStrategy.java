package server.app;

import protocol.IMessage;

public interface ICacheDisplacementStrategy {
  IMessage.K evict();
  void register(IMessage.K key);
}
