package server.storage.cache;

import protocol.K;

import java.util.LinkedHashSet;

public class LRU implements ICacheDisplacementTracker {
  private LinkedHashSet<K> registry;

  public LRU(int trackerCapacity) {
    this.registry = new LinkedHashSet<>(trackerCapacity + 1, 1);
  }

  @Override
  public K evict() {
    K k = registry.iterator().next();
    registry.iterator().remove();
    registry.remove(k);
    return k;
  }

  @Override
  public K register(K k) {
    registry.remove(k);
    registry.add(k);
    return k;
  }

  @Override
  public void unregister(K k) {
    this.registry.remove(k);
  }

  @Override
  public boolean containsKey(K key) {
    return registry.contains(key);
  }

}
