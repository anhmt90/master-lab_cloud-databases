package server.storage.cache;

import protocol.kv.K;

import java.util.LinkedHashSet;

/**
 * First In First Out strategy.
 * Evicts ({@link ICacheDisplacementTracker#evict()}) the oldest item in the insert order.
 */
public class FIFO implements ICacheDisplacementTracker {
    private LinkedHashSet<K> registry;

    public FIFO(int trackerCapacity) {
        registry = new LinkedHashSet<K>(trackerCapacity + 1, 1);
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
    public synchronized void  unregister(K k) {
        this.registry.remove(k);
    }

    @Override
    public boolean containsKey(K key) {
        return registry.contains(key);
    }
}
