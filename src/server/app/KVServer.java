package server.app;

import server.storage.disk.PersistenceManager;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static util.StringUtils.PATH_SEP;

public class KVServer {
    /**
     * Start KV ServerManager at given port
     *
     * @param port      given port for disk server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the storage replacement strategy in case the storage
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the storage. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {

    }

}
