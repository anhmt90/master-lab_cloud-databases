package server.storage.Cache;

import protocol.K;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LFU implements ICacheDisplacementStrategy {
  private HashMap<K, Counter> registry = new HashMap<>(1000);

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
      return min.get().getKey();
    }
    return null;
  }

  @Override
  public void register(K k) {
    Counter c = registry.get(k);

    if (c == null) {
      registry.put(k, new Counter());
    } else {
      c.inc();
    }
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
  public void get(K k) {
    register(k);
  }
}
