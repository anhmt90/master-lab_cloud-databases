package server.storage.cache;

import protocol.K;

import java.util.Iterator;
import java.util.LinkedHashSet;

public class FIFO implements ICacheDisplacementTracker {
  private LinkedHashSet<K> registry = new LinkedHashSet<>(1000);

  @Override
  public K evict() {
    Iterator<K> iter = registry.iterator();
    K k = iter.next();
    iter.remove();
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
