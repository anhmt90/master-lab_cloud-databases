package server.app;

import server.storage.PersistenceManager;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static util.StringUtils.PATH_SEP;

public class KVServer {
    public static final String ROOT_DB_PATH = System.getProperty("user.dir") + PATH_SEP +"db";

    PersistenceManager persistenceManager;

    /**
     * Start KV ServerManager at given port
     *
     * @param port      given port for storage server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed
     *                  to keep in-memory
     * @param strategy  specifies the cache replacement strategy in case the cache
     *                  is full and there is a GET- or PUT-request on a key that is
     *                  currently not contained in the cache. Options are "FIFO", "LRU",
     *                  and "LFU".
     */
    public KVServer(int port, int cacheSize, String strategy) {

    }

    @PostConstruct
    public void init() {
        persistenceManager = new PersistenceManager();
        createDBDir(ROOT_DB_PATH);
    }

    public boolean createDBDir(String path) {
        Path dbPath = Paths.get(path);
        if (!Files.exists(dbPath)) {
            try {
                Files.createDirectories(dbPath);
                System.out.println("New Directory Successfully Created !"); //TODO change to LOG
                return true;
            } catch (IOException ioe) {
                System.out.println("Problem occured while creating 'db' directory = " + ioe.getMessage()); //TODO change to LOG
            }
        }
        return false;
    }

}
