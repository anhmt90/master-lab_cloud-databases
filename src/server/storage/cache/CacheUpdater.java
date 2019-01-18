package server.storage.cache;

import protocol.kv.K;
import protocol.kv.V;

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
