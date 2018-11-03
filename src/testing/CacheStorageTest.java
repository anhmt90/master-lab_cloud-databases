package testing;

import junit.framework.TestCase;
import org.junit.Test;
import protocol.IMessage;
import server.storage.Cache.CacheStorage;
import server.storage.Cache.FIFO;
import server.storage.Cache.LFU;
import server.storage.Cache.LRU;

public class CacheStorageTest extends TestCase {
  IMessage.V testVal = new IMessage.V("test".getBytes());

  private void testPut(CacheStorage cache) {
    for (int i = 0; i < 10; i++) {
      IMessage.K k = new IMessage.K(Integer.toString(i).getBytes());
      IMessage.K kr = cache.put(k, testVal);

      assertNotNull(kr);
    }
  }

  private void testGet(CacheStorage cache) {
    for (int i = 0; i < 10; i++) {
      IMessage.K k = new IMessage.K(Integer.toString(i).getBytes());
      cache.put(k, testVal);
    }

    for (int i = 0; i < 10; i++) {
      IMessage.K k = new IMessage.K(Integer.toString(i).getBytes());
      IMessage.V v = cache.get(k);

      assertNotNull(v);
      assertEquals(testVal.get(), v.get());
    }
  }

  @Test
  public void testFifoPut() {
    CacheStorage cache = new CacheStorage(10);
    cache.setStrategy(new FIFO());

    testPut(cache);
  }

  @Test
  public void testLfuPut() {
    CacheStorage cache = new CacheStorage(10);
    cache.setStrategy(new LFU());

    testPut(cache);
  }

  @Test
  public void testLruPut() {
    CacheStorage cache = new CacheStorage(10);
    cache.setStrategy(new LRU());

    testPut(cache);
  }

  @Test
  public void testFifoGet() {
    CacheStorage cache = new CacheStorage(10);
    cache.setStrategy(new FIFO());

    testGet(cache);
  }

  @Test
  public void testLfuGet() {
    CacheStorage cache = new CacheStorage(10);
    cache.setStrategy(new LFU());

    testGet(cache);
  }

  @Test
  public void testLruGet() {
    CacheStorage cache = new CacheStorage(10);
    cache.setStrategy(new LRU());

    testGet(cache);
  }

}
