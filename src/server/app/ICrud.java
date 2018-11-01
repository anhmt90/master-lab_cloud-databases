package server.app;

import protocol.IMessage;
import protocol.Message;

public interface ICrud {
  IMessage get(IMessage.K key);
  IMessage put(IMessage msg);
}
