package server.storage.cache;

import protocol.K;
import protocol.V;
import util.Validate;

import java.util.concurrent.ConcurrentHashMap;

class CacheUpdater implements Runnable{
    private CacheManager cm;

    private K key;
    private V val;

    CacheUpdater(CacheManager cm, K key, V val) {
        this.cm = cm;
        this.key = key;
        this.val = val;
    }

    @Override
    public void run() {
        cm.updateCache(key, val);
    }
}
