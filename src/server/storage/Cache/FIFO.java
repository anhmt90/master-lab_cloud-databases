package server.storage.Cache;

import protocol.IMessage;
import protocol.K;

import java.util.LinkedHashSet;

public class FIFO implements ICacheDisplacementStrategy {
  private LinkedHashSet<K> registry = new LinkedHashSet<>(1000);

  @Override
  public K evict() {
    K k = registry.iterator().next();
    registry.iterator().remove();
    registry.remove(k);
    return k;
  }

  @Override
  public void register(K k) {
    registry.remove(k);
    registry.add(k);
  }

  @Override
  public void unregister(K k) {
    this.registry.remove(k);
  }

  @Override
  public void put(K k) {
    register(k);
  }

  @Override
  public void get(K k) {}

}
