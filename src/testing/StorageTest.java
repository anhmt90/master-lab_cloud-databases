package testing;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import protocol.K;
import protocol.V;
import server.storage.cache.CacheStorage;
import server.storage.cache.FIFO;
import server.storage.cache.LFU;
import server.storage.cache.LRU;

public class StorageTest extends TestCase {
    String KEY = "SomeKey";
    String VAL = "SomeValue";
    String NEW_KEY = "UpdatedKey";

    private K key;
    private V val;

    @Before
    public void init() {
        key = new K(KEY.getBytes());
        val = new V(VAL.getBytes());
    }

    private void initServer(int cacheSize, String strategy) {

    }
}
