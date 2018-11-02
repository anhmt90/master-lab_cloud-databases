package server.app;

import protocol.IMessage;

public class PersistentStorage implements ICrud {
  public PersistentStorage(String path) {
  }

  @Override
  public IMessage get(IMessage.K key) {
    return null;
  }

  @Override
  public IMessage put(IMessage msg) {
    return null;
  }
}
