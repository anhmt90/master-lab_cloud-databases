package server.storage.cache;

import protocol.K;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LFU implements ICacheDisplacementTracker {
  private HashMap<K, Counter> registry;

  public LFU(int trackerCapacity) {
    this.registry = new HashMap<>(trackerCapacity + 1, 1);
  }

  private class Counter implements Comparable<Counter> {
    private int value = 1;
    void inc() {
      value++;
    }
    int getValue() {
      return value;
    }

    @Override
    public int compareTo(Counter counter) {
      return value - counter.getValue();
    }
  }

  @Override
  public K evict() {
    Optional<Map.Entry<K, Counter>> min = registry.entrySet()
        .stream()
        .min(Map.Entry.comparingByValue());

    if (min.isPresent()) {
      K k = min.get().getKey();
      this.registry.remove(k);
      return k;
    }
    return null;
  }

  @Override
  public K register(K k) {
    Counter c = registry.get(k);

    if (c == null) {
      registry.put(k, new Counter());
    } else {
      c.inc();
    }
    return k;
  }

  @Override
  public void unregister(K k) {
    this.registry.remove(k);
  }

  @Override
  public boolean containsKey(K key) {
    return registry.containsKey(key);
  }
}
