package testing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import protocol.kv.K;
import protocol.kv.V;
import server.storage.cache.CacheManager;
import server.storage.cache.CacheDisplacementStrategy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Test cases for cache and its displacement strategies
 */
public class CacheTest {
	private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

  private class TestClient extends Thread {
    private final int from;
    private final int to;
    private final Method updateCache;
    private final CacheManager cm;

    TestClient(int from, int to, CacheManager cm, Method updateCache) {
      this.from = from;
      this.to = to;
      this.updateCache = updateCache;
      this.cm = cm;
    }

    public void run() {
      for (int i = from; i < to; i++) {
        K key = new K(Integer.toString(i));
        String valStr = "test" + Integer.toString(i);
        V val = new V(valStr);
        try {
          this.updateCache.invoke(cm, key, val);
        } catch (IllegalAccessException | InvocationTargetException e) {
        	LOG.error(e);
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * Get private method for updating the cache from the {@link CacheManager}
   * @param cm CacheManager
   * @return {@link CacheManager#updateCache(K, V)}
   * @throws NoSuchMethodException something was changed in the implementation and method doesn't exist anymore
   */
  private Method getUpdateCacheMethod(CacheManager cm) throws NoSuchMethodException {
    Class<? extends CacheManager> cl = cm.getClass();
    Method updateCache = cl.getDeclaredMethod("updateCache", K.class, V.class);
    updateCache.setAccessible(true);

    return updateCache;
  }

  /**
   * Test simple put operation on cache without exceeding the capacity
   * @param capacity capacity of the cache to test
   * @param strategy displacemetn strategy to test
   * @throws NoSuchMethodException for some reason {@link CacheManager#updateCache(K, V)} was not found
   * @throws InterruptedException if {@link TestClient} thread was interrupted
   */
  private void testCacheStrategySimplePut(int capacity, CacheDisplacementStrategy strategy) throws NoSuchMethodException, InterruptedException {
    CacheManager cm = new CacheManager(AllTests.DB_DIR, capacity, strategy);

    Method updateCache = getUpdateCacheMethod(cm);

    List<TestClient> clients = new ArrayList<>();
    int keysPerClient = 10;
    for (int i = 0; i < cm.getCacheCapacity() / keysPerClient; i++) {
      TestClient c = new TestClient(i * keysPerClient, (i + 1) * keysPerClient, cm, updateCache);
      clients.add(c);
      c.start();
    }

    for (TestClient c: clients){
      c.join();
    }

    int to = cm.getCacheCapacity() / keysPerClient;
    to *= keysPerClient;
    for (int i = 0; i < to; i++) {
      K key = new K(Integer.toString(i));
      assertTrue(String.format("Key \"%d\" is missing", i), cm.getCache().containsKey(key));
    }
  }

  /**
   * Test simple put for {@link server.storage.cache.FIFO} strategy {@link #testCacheStrategySimplePut(int, CacheDisplacementStrategy)}
   */
  @Test
  public void testFIFOsimplePut() throws NoSuchMethodException, InterruptedException {
    testCacheStrategySimplePut(1000, CacheDisplacementStrategy.FIFO);
  }

  /**
   * Test simple put for {@link server.storage.cache.LRU} strategy {@link #testCacheStrategySimplePut(int, CacheDisplacementStrategy)}
   */
  @Test
  public void testLRUsimplePut() throws NoSuchMethodException, InterruptedException {
    testCacheStrategySimplePut(1000, CacheDisplacementStrategy.LRU);
  }

  /**
   * Test simple put for {@link server.storage.cache.LFU} strategy {@link #testCacheStrategySimplePut(int, CacheDisplacementStrategy)}
   */
  @Test
  public void testLFUsimplePut() throws NoSuchMethodException, InterruptedException {
    testCacheStrategySimplePut(1000, CacheDisplacementStrategy.LFU);
  }

  /**
   * Test one of the edge cases when there is a cache of loadedDataSize one, check if eviction works
   * @param strategy {@link CacheDisplacementStrategy}
   */
  private void testCacheOneItemEvict(CacheDisplacementStrategy strategy) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    CacheManager cm = new CacheManager(AllTests.DB_DIR, 1, strategy);
    Method updateCache = getUpdateCacheMethod(cm);

    V v = new V("testValue");
    String k1Str = "Test1";
    String k2Str = "Test2";
    K k1 = new K(k1Str);
    K k2 = new K(k2Str);

    updateCache.invoke(cm, k1, v);
    assertEquals(1, cm.getCache().size());
    assertTrue(String.format("Added key \"%s\" is missing", k1Str), cm.getCache().containsKey(k1));
    updateCache.invoke(cm, k2, v);
    assertEquals(1, cm.getCache().size());
    assertTrue(String.format("Added key \"%s\" is missing", k2Str), cm.getCache().containsKey(k2));
  }

  /**
   * Eviction test {@link #testCacheOneItemEvict(CacheDisplacementStrategy)} for {@link server.storage.cache.FIFO} strategy
   */
  @Test
  public void testFIFOoneItemEvict() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    testCacheOneItemEvict(CacheDisplacementStrategy.FIFO);
  }

  /**
   * Eviction test {@link #testCacheOneItemEvict(CacheDisplacementStrategy)} for {@link server.storage.cache.LRU} strategy
   */
  @Test
  public void testLRUoneItemEvict() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    testCacheOneItemEvict(CacheDisplacementStrategy.LRU);
  }

  /**
   * Eviction test {@link #testCacheOneItemEvict(CacheDisplacementStrategy)} for {@link server.storage.cache.LFU} strategy
   */
  @Test
  public void testLFUoneItemEvict() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    testCacheOneItemEvict(CacheDisplacementStrategy.LFU);
  }

  /**
   * Test removing an item from the cache when value of put operations is null
   * @param cm {@link CacheManager} to test
   */
  private void testDelete(CacheManager cm) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    Method updateCache = getUpdateCacheMethod(cm);

    V v = new V("testValue");
    String kStr = "Test1";
    K k = new K(kStr);

    updateCache.invoke(cm, k, v);
    assertEquals(1, cm.getCache().size());
    updateCache.invoke(cm, k, null);
    assertEquals(0, cm.getCache().size());
  }

  /**
   * Cache item remove operation for cache {@link #testDelete(CacheManager)} with {@link server.storage.cache.FIFO} strategy
   */
  @Test
  public void testFIFODelete() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    CacheManager cm = new CacheManager(AllTests.DB_DIR, 100, CacheDisplacementStrategy.FIFO);
    testDelete(cm);
  }

  /**
   * Cache item remove operation for cache {@link #testDelete(CacheManager)} with {@link server.storage.cache.LRU} strategy
   */
  @Test
  public void testLRUDelete() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    CacheManager cm = new CacheManager(AllTests.DB_DIR, 100, CacheDisplacementStrategy.LRU);
    testDelete(cm);
  }

  /**
   * Cache item remove operation for cache {@link #testDelete(CacheManager)} with {@link server.storage.cache.LFU} strategy
   */
  @Test
  public void testLFUDelete() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
    CacheManager cm = new CacheManager(AllTests.DB_DIR, 100, CacheDisplacementStrategy.LFU);
    testDelete(cm);
  }

}
