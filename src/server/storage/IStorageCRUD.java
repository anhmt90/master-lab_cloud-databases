package server.storage;

import protocol.kv.K;
import protocol.kv.V;

/**
 * CRUD interface
 * Implement to comply with operations used in Key-Value store
 */
public interface IStorageCRUD {
    V get(K key, String MRJobId);

    PUTStatus put(K key, V val, String MRJobId);
}
