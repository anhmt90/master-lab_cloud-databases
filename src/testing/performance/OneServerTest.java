package testing.performance;

import client.api.Client;
import ecs.ExternalConfigurationService;
import ecs.KVServer;
import util.FileUtils;

import java.io.IOException;

import static util.FileUtils.SEP;

public class OneServerTest {
  private static final String enronPath = FileUtils.WORKING_DIR + "/../../maildir";
  private EnronDataset enronDataset;
  private static final String ecsConfigPath = System.getProperty("user.dir") + SEP + "config" + SEP + "server-info";
  private ExternalConfigurationService ecs;
  private Stopwatch sw = new Stopwatch();

  public void init() throws IOException, NoSuchFieldException, IllegalAccessException {
    ecs = new ExternalConfigurationService(ecsConfigPath);
    enronDataset = new EnronDataset(enronPath);
    enronDataset.loadAllMessages();
  }

  public void testCacheToTime() throws Exception {
    int numClients = 100;
    int opsPerClient = 1000;
    int putRatio = 50;
    int[] cacheSizes = new int[5];
    cacheSizes[0] = 1;
    for (int i = 1; i < cacheSizes.length; i++) {
      cacheSizes[i] = cacheSizes[i-1] * 10;
    }
    String[] strategies = {"FIFO", "LFU", "LRU"};

    ClientRunner[] clients = new ClientRunner[numClients];

    for (String strategy: strategies) {
      for (int cacheSize: cacheSizes) {
        ecs.initService(1, cacheSize, strategy);
        KVServer kvS = ecs.getChord().nodes().iterator().next();
        for (int i = 0; i < clients.length; i++) {
          Client client = new Client(kvS.getHost(), kvS.getPort());
          clients[i] = new ClientRunner(client, enronDataset, putRatio, opsPerClient);
        }

        Thread[] threads = new Thread[clients.length];
        for (int i = 0; i < clients.length; i++) {
          ClientRunner clientRunner = clients[i];
          threads[i] = new Thread(clientRunner);
        }

        sw.tick();
        for (Thread t: threads) {
          t.start();
        }

        for (Thread t: threads) {
          t.join();
        }
        long elapsedTime = sw.tick();
        long totalOps = numClients * opsPerClient;

        ecs.shutDown();
      }
    }
  }
}
