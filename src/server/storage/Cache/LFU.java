package server.storage.Cache;

import protocol.IMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class LFU implements ICacheDisplacementStrategy {
  private HashMap<IMessage.K, Counter> registry = new HashMap<>(1000);

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
  public IMessage.K evict() {
    Optional<Map.Entry<IMessage.K, Counter>> min = registry.entrySet()
        .stream()
        .min(Map.Entry.comparingByValue());

    if (min.isPresent()) {
      return min.get().getKey();
    }
    return null;
  }

  @Override
  public void register(IMessage.K key) {
    Counter c = registry.get(key);

    if (c == null) {
      registry.put(key, new Counter());
    } else {
      c.inc();
    }
  }
}
