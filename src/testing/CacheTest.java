package testing;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import protocol.K;
import protocol.V;
import server.storage.CacheManager;
import server.storage.cache.CacheDisplacementStrategy;
import server.storage.cache.FIFO;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class CacheTest extends TestCase {

  private class TestClient extends Thread {
    private final int from;
    private final int to;

    public TestClient(int from, int to, Method updateCache) {
      this.from = from;
      this.to = to;
    }

    public void run() {
      for (int i = from; i < to; i++) {
      }
    }
  }

  @Test
  public void testFIFO() throws NoSuchMethodException {
    CacheManager cm = new CacheManager(100, CacheDisplacementStrategy.FIFO);

    Class<? extends CacheManager> cl = cm.getClass();
    Method updateCache = cl.getDeclaredMethod("updateCache", K.class, V.class);
    updateCache.setAccessible(true);

    updateCache.invoke();
  }

  @Test
  public void testLRU() {
  }

  @Test
  public void testLFU() {
  }
}
