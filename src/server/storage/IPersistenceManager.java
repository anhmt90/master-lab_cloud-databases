package server.storage;

import java.util.concurrent.ConcurrentHashMap;

public interface IPersistenceManager {

    /*
    * Since we currently have only one server, sharding strategy can be simply based on
    * client IP address and the key.
    * */
//    ConcurrentHashMap KVHashTable;


    boolean write(String key, String value);
    String read(String key);

}
