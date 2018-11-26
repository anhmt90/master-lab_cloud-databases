package testing.performance;

import client.api.Client;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

class ClientRunner implements Runnable {

  private final Client client;
  private final int putRatio;
  private final int ops;
  private final EnronDataset enronDataset;

  public ClientRunner(Client client, EnronDataset enronDataset, int putRatio, int ops) {
    this.enronDataset = enronDataset;
    this.client = client;
    this.putRatio = putRatio;
    this.ops = ops;
  }

  @Override
  public void run() {
    int n = ThreadLocalRandom.current().nextInt(100);
    EnronDataset.KV kv = enronDataset.getRandom();
    for (int i = 0; i < ops; i++) {
      try {
        if (n >= putRatio) {
          client.put(kv.key, kv.val);
        } else {
          client.put(kv.key, kv.val);
        }
      } catch(IOException e){
        e.printStackTrace();
      }
    }
  }

}
