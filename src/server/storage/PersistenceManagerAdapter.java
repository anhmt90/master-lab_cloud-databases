package server.storage;

import protocol.IMessage;
import server.storage.disk.IPersistenceManager;
import server.storage.disk.PersistenceManager;

public class PersistenceManagerAdapter extends PersistentStorage {
  private PersistenceManager pm;
  public PersistenceManagerAdapter(String path) {
    super(path);
    pm = new PersistenceManager();
  }

  @Override
  public IMessage.V get(IMessage.K key) {
    /* TODO: why is it returning String instead of OpResult compared to other operations?
    TODO: will it actually always return null if there is no key in disk storage?
     */
    String readResult = pm.read(key);

    if (readResult == null) {
      return null;
    }

    return new IMessage.V(readResult.getBytes());
  }

  @Override
  public IMessage.K put(IMessage.K key, IMessage.V val) {
    IPersistenceManager.OpStatus opStatus;

    if (val == null) {
      opStatus = pm.delete(key);
      switch (opStatus) {
        case DELETE_ERR:
          return null;
        case DELETE_SUCCESS:
          return key;
      }
    }

    opStatus = pm.write(key, val);
    switch (opStatus) {
      case WRITE_ERR:
        return null;
      case WRITE_SUCCESS:
        return key;
    }

    return null;
  }
}
