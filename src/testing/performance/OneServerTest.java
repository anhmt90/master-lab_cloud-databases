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
    enronDataset.loadMessages(3000);
  }

  public long[] testServersClients(int numClients, int putRatio, int opsPerClient,
                                 int numServers, int cacheSize, String strategy) throws Exception {

    ClientRunner[] clients = new ClientRunner[numClients];
    ecs.initService(numServers, cacheSize, strategy);
    ecs.startService();
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
    long maxLatency = 0;
    long minLatency = Long.MAX_VALUE;
    for (ClientRunner cr: clients) {
      if (cr.getMaxLatency() > maxLatency) maxLatency = cr.getMaxLatency();
      if (cr.getMaxLatency() < minLatency) minLatency = cr.getMinLatency();
    }
    ecs.shutDown();

    return new long[]{elapsedTime, minLatency, maxLatency};
  }

  public void testCacheToTime(int numClients, int opsPerClient, int putRatio) throws Exception {
    ReportBuilder rb = new ReportBuilder();
    long totalOps = numClients * opsPerClient;

    rb.addValue("num_clients");
    rb.addValue("ops_per_client");
    rb.addValue("put_ratio");
    rb.addSb();

    rb.addValue(numClients);
    rb.addValue(opsPerClient);
    rb.addValue(putRatio);
    rb.addSb();

    int[] cacheSizes = new int[5];
    cacheSizes[0] = 1;
    for (int i = 1; i < cacheSizes.length; i++) {
      cacheSizes[i] = cacheSizes[i-1] * 10;
    }
    String[] strategies = {"FIFO", "LFU", "LRU"};

    for (String strategy: strategies) {
      rb.addHeader(strategy);
      long[] times = new long[cacheSizes.length];
      long[] minLatencies = new long[cacheSizes.length];
      long[] maxLatencies = new long[cacheSizes.length];
      long[] opsPerSec = new long[cacheSizes.length];

      for (int i = 0; i < cacheSizes.length; i++) {
        int cacheSize = cacheSizes[i];
        long[] results = testServersClients(numClients, putRatio, opsPerClient, 1, cacheSize, strategy);
        times[i] = results[0];
        minLatencies[i] = results[1];
        maxLatencies[i] = results[2];
        double seconds = (double)results[0]/ 1_000_000_000.0;
        long opps = (long) (seconds / totalOps);
        opsPerSec[i] = opps;
      }

      rb.addHeader("cache_size");
      rb.addValues(cacheSizes);
      rb.addHeader("elapsed_time");
      rb.addValues(times);
      rb.addHeader("min_latency");
      rb.addValues(minLatencies);
      rb.addHeader("max_latency");
      rb.addValues(maxLatencies);
      rb.addHeader("ops_per_second");
      rb.addValues(opsPerSec);
    }

    String outputPath = FileUtils.WORKING_DIR  + SEP + "performance_results" + SEP + "cache_to_time.txt";
    rb.writeToFile(outputPath);
  }

  public void testDifferentNumberClientsServers(int[] serverNums, String strategy, int cacheSize,
                                                int[] clientNums, int putRatio, int opsPerClient) throws Exception {
    ReportBuilder rb = new ReportBuilder();
    for (int cn: clientNums) {
      rb.addHeader("clients_number");
      rb.addValue(cn);
      rb.addSb();
      long[] times = new long[serverNums.length];
      long[] minLatencies = new long[serverNums.length];
      long[] maxLatencies = new long[serverNums.length];
      long[] opsPerSec = new long[serverNums.length];
      for (int i = 0; i < serverNums.length; i++) {
        int sn = clientNums[i];
        long totalOps = cn * opsPerClient;
        long[] results = testServersClients(cn, putRatio, opsPerClient, sn, cacheSize, strategy);
        times[i] = results[0];
        minLatencies[i] = results[1];
        maxLatencies[i] = results[2];
        double seconds = (double)results[0]/ 1_000_000_000.0;
        long opps = (long) (seconds / totalOps);
        opsPerSec[i] = opps;
      }
      rb.addHeader("servers_number");
      rb.addValues(serverNums);
      rb.addHeader("elapsed_time");
      rb.addValues(times);
      rb.addHeader("min_latency");
      rb.addValues(minLatencies);
      rb.addHeader("max_latency");
      rb.addValues(maxLatencies);
      rb.addHeader("ops_per_second");
      rb.addValues(opsPerSec);
    }
    String outputPath = FileUtils.WORKING_DIR  + SEP + "performance_results" + SEP + "servers_clients.txt";
    rb.writeToFile(outputPath);
  }

  public static void main(String[] args) {
    OneServerTest ost = new OneServerTest();
    try {
      ost.init();
    } catch (IOException | NoSuchFieldException | IllegalAccessException e) {
      e.printStackTrace();
    }

    int numClients = 100;
    int opsPerClient = 1000;
    int putRatio = 50;

    try {
      ost.testCacheToTime(numClients, opsPerClient, putRatio);
    } catch (Exception e) {
      e.printStackTrace();
    }

    int[] numClientsss = new int[]{1, 5, 20};
    int[] numServersss = new int[]{1, 5, 10};
    int cacheSize = 100;
    String strategy = "LFU";
    try {
      ost.testDifferentNumberClientsServers(numServersss, strategy, cacheSize, numClientsss, putRatio, opsPerClient);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
