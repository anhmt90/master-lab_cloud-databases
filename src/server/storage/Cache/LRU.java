package server.storage.Cache;

import protocol.IMessage;

import java.util.LinkedHashSet;

public class LRU implements ICacheDisplacementStrategy {
  private LinkedHashSet<IMessage.K> registry = new LinkedHashSet<>(1000);

  @Override
  public IMessage.K evict() {
    IMessage.K k = registry.iterator().next();
    registry.iterator().remove();
    registry.remove(k);
    return k;
  }

  @Override
  public void register(IMessage.K k) {
    registry.remove(k);
    registry.add(k);
  }

  @Override
  public void unregister(IMessage.K k) {
    this.registry.remove(k);
  }

  @Override
  public void put(IMessage.K k) {
    registry.remove(k);
    register(k);
  }

  @Override
  public void get(IMessage.K k) {
    register(k);
  }
}
