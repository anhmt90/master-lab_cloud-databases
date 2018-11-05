package server.storage.disk;

import server.storage.PUTStatus;

public interface IPersistenceManager {

    PUTStatus write(byte[] key, byte[] value);

    byte[] read(byte[] key);

    PUTStatus delete(byte[] key);

}
