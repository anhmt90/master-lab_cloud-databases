package app_kvServer;

import common.messages.KVMessage;

public class PersistentStorage implements ICrud {
  public PersistentStorage(String path) {
  }

  @Override
  public KVMessage get(String key) {
    return null;
  }

  @Override
  public KVMessage put(String key, String value) {
    return null;
  }
}
