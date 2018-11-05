package testing;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import protocol.K;
import protocol.V;
import server.storage.CacheManager;
import server.storage.cache.CacheDisplacementStrategy;
import server.storage.cache.FIFO;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class CacheTest extends TestCase {

  private class TestClient extends Thread {
    private final int from;
    private final int to;
    private final Method updateCache;

    public TestClient(int from, int to, Method updateCache) {
      this.from = from;
      this.to = to;
      this.updateCache = updateCache;
    }

    public void run() {
      for (int i = from; i < to; i++) {
        K key = new K(Integer.toString(i).getBytes());
        String valStr = "test" + Integer.toString(i);
        V val = new V(valStr.getBytes());
        try {
          this.updateCache.invoke(key, val);
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        } catch (InvocationTargetException e) {
          e.printStackTrace();
        }
      }
    }
  }

  private void testCache(int capacity, CacheDisplacementStrategy strategy) throws NoSuchMethodException {
    CacheManager cm = new CacheManager(capacity, strategy);

    Class<? extends CacheManager> cl = cm.getClass();
    Method updateCache = cl.getDeclaredMethod("updateCache", K.class, V.class);
    updateCache.setAccessible(true);

    int keysPerClient = 10;
    for (int i = 0; i < cm.getCacheCapacity() / keysPerClient; i++) {
      new TestClient(i * keysPerClient, (i + 1) * keysPerClient, updateCache).start();
    }

    int to = cm.getCacheCapacity() / keysPerClient;
    to *= keysPerClient;
    for (int i = 0; i < to; i++) {
      K key = new K(Integer.toString(i).getBytes());
      assertTrue(cm.getCache().contains(key));
    }
  }

  @Test
  public void testFIFO() throws NoSuchMethodException {
    testCache(1000, CacheDisplacementStrategy.FIFO);
  }

  @Test
  public void testLRU() throws NoSuchMethodException {
    testCache(1000, CacheDisplacementStrategy.LRU);
  }

  @Test
  public void testLFU() throws NoSuchMethodException {
    testCache(1000, CacheDisplacementStrategy.LFU);
  }
}
