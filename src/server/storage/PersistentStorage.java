package server.storage;

import protocol.IMessage;

public class PersistentStorage implements ICrud {
  public PersistentStorage(String path) {
  }

  @Override
  public IMessage.V get(IMessage.K key) {
    return null;
  }

  @Override
  public IMessage.K put(IMessage.K key, IMessage.V val) {
    return null;
  }
}
