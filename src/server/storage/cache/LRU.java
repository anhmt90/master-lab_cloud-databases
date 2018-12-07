package server.storage.cache;

import protocol.K;

import java.util.LinkedHashSet;

/**
 * Least Recently Used strategy.
 * Evicts ({@link ICacheDisplacementTracker#evict()}) the oldest item in the access order.
 * Access - put ot get operations.
 */
public class LRU implements ICacheDisplacementTracker {
  private LinkedHashSet<K> registry;

  public LRU(int trackerCapacity) {
    this.registry = new LinkedHashSet<>(trackerCapacity + 1, 1);
  }

  @Override
  public synchronized K evict() {
    K k = registry.iterator().next();
    registry.remove(k);
    return k;
  }

  @Override
  public synchronized K register(K k) {
    registry.remove(k);
    registry.add(k);
    return k;
  }

  @Override
  public synchronized void unregister(K k) {
    this.registry.remove(k);
  }

  @Override
  public boolean containsKey(K key) {
    return registry.contains(key);
  }

}
