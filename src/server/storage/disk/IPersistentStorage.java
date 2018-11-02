package server.storage.disk;

import protocol.IMessage;

public interface IPersistentStorage {

    public enum OpStatus {
        WRITE_SUCCESS("File successfully created/updated"),
        DELETE_SUCCESS("File successfully removed"),

        WRITE_ERR("Error while creating/updating file"),
        DELETE_ERR("Error while removed file");

        String descr;

        OpStatus(String descr) {
            this.descr = descr;
        }
    }

    /*
    * Since we currently have only one server, sharding strategy can be simply based on
    * client IP address and the key.
    * */
//    ConcurrentHashMap KVHashTable;


    OpStatus write(IMessage.K key, IMessage.V value);
    String read(IMessage.K k);
    OpStatus delete(IMessage.K key);

}
