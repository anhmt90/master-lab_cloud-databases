package testing;

import client.api.Client;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Test;
import protocol.IMessage;
import protocol.IMessage.Status;
import protocol.K;
import protocol.V;
import server.app.Server;
import server.storage.CacheManager;
import server.storage.cache.CacheDisplacementStrategy;
import server.storage.cache.FIFO;
import server.storage.disk.PersistenceManager;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

public class StorageIntegrationTest extends TestCase {
    String KEY1 = "Key1";
    String VAL1 = "Val1";
    String KEY2 = "Key2";
    String VAL2 = "Val2";
    String KEY3 = "Key3";
    String VAL3 = "Val3";

    Server kvServer;
    Client kvClient;
    static final int CACHE_SIZE = 2;

    private K key1;
    private K key2;
    private K key3;
    private V val1;
    private V val2;
    private V val3;

    public void init(int cacheSize, CacheDisplacementStrategy strategy) throws IOException {
        given_3_KV_pairs();
        kvServer = new Server(50000, cacheSize, strategy, "ERROR");
        kvServer.start();
        cacheManager = kvServer.getCacheManager();
        cache = cacheManager.getCache();
        pm = cacheManager.getPersistenceManager();

        kvClient = new Client("localhost", 50000);
        connectServer(kvClient);
    }

    private void given_3_KV_pairs() {
        key1 = new K(KEY1.getBytes());
        key2 = new K(KEY2.getBytes());
        key3 = new K(KEY3.getBytes());
        val1 = new V(VAL1.getBytes());
        val2 = new V(VAL2.getBytes());
        val3 = new V(VAL3.getBytes());
    }

    private void connectServer(Client kvClient) throws IOException {
        try {
            kvClient.connect();
            byte[] bytes = kvClient.receive();
            String welcomeMessage = new String(bytes, StandardCharsets.US_ASCII).trim();
            System.out.println(welcomeMessage);
        } catch (IOException e) {
            throw e;
        }
    }

    @After
    public void tearDown() {
        if (kvServer != null)
            kvServer.stopServer();
    }


    CacheManager cacheManager;
    ConcurrentHashMap<K, V> cache;
    PersistenceManager pm;

    @Test
    public void testFIFOSingleThread() throws IOException {
        init(CACHE_SIZE, CacheDisplacementStrategy.FIFO);

        assertThat(cacheManager.getCacheCapacity(), is(CACHE_SIZE));
        assertThat(cacheManager.getCacheTracker() instanceof FIFO, is(true));

        IMessage putRes1 = kvClient.put(KEY1, VAL1);
        assertPutSuccessOrPutUpdate(putRes1);
        assertThat(cache.mappingCount(), is(1L));
        assertThat(cache.containsKey(putRes1.getKey()), is(true));
        assertThat(val1.equals(cache.get(putRes1.getKey())), is(true));


        IMessage putRes2 = kvClient.put(KEY2, VAL2);
        assertPutSuccessOrPutUpdate(putRes2);
        assertThat(cache.mappingCount(), is(2L));
        assertThat(cache.containsKey(putRes2.getKey()), is(true));
        assertThat(cache.containsKey(putRes1.getKey()), is(true));
        assertThat(val2.equals(cacheManager.getCache().get(putRes2.getKey())), is(true));


        IMessage putRes3 = kvClient.put(KEY3, VAL3);
        assertPutSuccessOrPutUpdate(putRes3);
        assertThat(cache.mappingCount(), is(2L));
        assertThat(cache.containsKey(putRes1.getKey()), is(false));
        assertThat(cache.containsKey(putRes2.getKey()), is(true));
        assertThat(cache.containsKey(putRes3.getKey()), is(true));
        assertThat(val3.equals(cacheManager.getCache().get(putRes3.getKey())), is(true));

        IMessage getRes1 = kvClient.get(KEY1);
        assertThat(getRes1.getStatus().equals(Status.GET_SUCCESS), is(true));
        assertThat(cache.mappingCount(), equalTo(2L));
        assertThat(cache.containsKey(putRes1.getKey()), is(true));
        assertThat(cache.containsKey(putRes2.getKey()), is(false));
        assertThat(cache.containsKey(putRes3.getKey()), is(true));
        assertThat(val1.equals(cacheManager.getCache().get(putRes1.getKey())), is(true));

        assertThat(pm.isExisted(KEY1), is(true));
        assertThat(pm.isExisted(KEY2), is(true));
        assertThat(pm.isExisted(KEY3), is(true));

        IMessage deleteRes1 = kvClient.put(KEY1);
        assertThat(deleteRes1.getStatus().equals(Status.DELETE_SUCCESS), is(true));
        assertThat(cache.mappingCount(), equalTo(1L));
        assertThat(cache.containsKey(key3), is(true));
        assertThat(pm.isExisted(KEY1), is(false));

        IMessage deleteRes2 = kvClient.put(KEY2);
        assertThat(deleteRes2.getStatus().equals(Status.DELETE_SUCCESS), is(true));
        assertThat(cache.mappingCount(), equalTo(1L));
        assertThat(cache.containsKey(key3), is(true));
        assertThat(pm.isExisted(KEY2), is(false));

        IMessage deleteRes3 = kvClient.put(KEY3);
        assertThat(deleteRes3.getStatus().equals(Status.DELETE_SUCCESS), is(true));
        assertThat(cache.mappingCount(), equalTo(0L));
        assertThat(pm.isExisted(KEY3), is(false));

    }

    private void assertPutSuccessOrPutUpdate(IMessage message) {
        boolean res = message.getStatus().equals(Status.PUT_SUCCESS) || message.getStatus().equals(Status.PUT_UPDATE);
        assertThat(res, is(true));
    }


}
