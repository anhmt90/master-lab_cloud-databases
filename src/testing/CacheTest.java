package testing;

import junit.framework.TestCase;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import protocol.K;
import protocol.V;
import server.storage.CacheManager;
import server.storage.cache.CacheDisplacementStrategy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Test cases for cache and its displacement strategies
 */
public class CacheTest extends TestCase {
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
        K key = new K(Integer.toString(i).getBytes());
        String valStr = "test" + Integer.toString(i);
        V val = new V(valStr.getBytes());
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
   * @throws NoSuchMethodException for some reqson {@link CacheManager#updateCache(K, V)} was not found
   * @throws InterruptedException if {@link TestClient} thread was interrupted
   */
  private void testCacheStrategySimplePut(int capacity, CacheDisplacementStrategy strategy) throws NoSuchMethodException, InterruptedException {
    CacheManager cm = new CacheManager(capacity, strategy);

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
      K key = new K(Integer.toString(i).getBytes());
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
   * Test one of the edge cases when there is a cache of size one, check if eviction works
   * @param strategy {@link CacheDisplacementStrategy}
   */
  private void testCacheOneItemEvict(CacheDisplacementStrategy strategy) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    CacheManager cm = new CacheManager(1, strategy);
    Method updateCache = getUpdateCacheMethod(cm);

    V v = new V("testValue".getBytes());
    String k1Str = "Test1";
    String k2Str = "Test2";
    K k1 = new K(k1Str.getBytes());
    K k2 = new K(k2Str.getBytes());

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
}
