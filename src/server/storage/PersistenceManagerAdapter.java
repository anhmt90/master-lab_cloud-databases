package server.storage;

import protocol.IMessage;
import protocol.K;
import protocol.V;
import server.storage.disk.IPersistentStorage;
import server.storage.disk.PersistentStorage;

public class PersistenceManagerAdapter extends server.storage.PersistentStorage {
  private PersistentStorage pm;
  public PersistenceManagerAdapter(String path) {
    super(path);
    pm = new PersistentStorage(path);
  }

  @Override
  public V get(K key) {
    /* TODO: why is it returning String instead of OpResult compared to other operations?
    TODO: will it actually always return null if there is no key in disk storage?
     */
    String readResult = pm.read(key);

    if (readResult == null) {
      return null;
    }

    return new V(readResult.getBytes());
  }

  @Override
  public K put(K key, V val) {
    IPersistentStorage.OpStatus opStatus;

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
