package testing.performance;

import client.api.Client;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

class ClientRunner implements Runnable {

  private final Client client;
  private final int putRatio;
  private final int ops;
  private final EnronDataset enronDataset;

  private long maxLatency = 0;
  private long minLatency = Long.MAX_VALUE;

  public long getMaxLatency() {
    return maxLatency;
  }

  public long getMinLatency() {
    return minLatency;
  }

  public ClientRunner(Client client, EnronDataset enronDataset, int putRatio, int ops) {
    this.enronDataset = enronDataset;
    this.client = client;
    this.putRatio = putRatio;
    this.ops = ops;
  }

  @Override
  public void run() {
    try {
      client.connect();
    } catch (IOException e) {
      e.printStackTrace();
    }
    Stopwatch sw = new Stopwatch();
    for (int i = 0; i < ops; i++) {
      int n = ThreadLocalRandom.current().nextInt(100);
      EnronDataset.KV kv = enronDataset.getRandom();
      try {
        sw.tick();
        if (n >= putRatio) {
          client.put(kv.key, kv.val);
        } else {
          client.put(kv.key, kv.val);
        }
        long tick = sw.tick();
        if (tick > maxLatency) maxLatency = tick;
        if (tick < minLatency) minLatency = tick;
      } catch(IOException e){
        e.printStackTrace();
      }
    }
  }

}
