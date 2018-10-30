package app_kvServer;

import common.messages.KVMessage;

public interface ICrud {
  KVMessage get(String key);
  KVMessage put(String key, String value);
}
