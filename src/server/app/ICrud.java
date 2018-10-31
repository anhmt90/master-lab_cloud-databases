package server.app;

import protocol.Message;

public interface ICrud {
  Message get(String key);
  Message put(String key, String value);
}
