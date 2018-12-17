package server.storage;

import protocol.K;
import protocol.V;

/**
 * CRUD interface
 * Implement to comply with operations used in Key-Value store
 */
public interface IStorageCRUD {
    V get(K key);

    PUTStatus put(K key, V val);
}
