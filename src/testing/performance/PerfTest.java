package testing.performance;

import client.api.Client;
import ecs.ExternalConfigurationService;
import ecs.KVServer;
import org.junit.Test;
import protocol.IMessage.Status;
import util.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static util.FileUtils.SEP;
import static util.FileUtils.USER_DIR;

public class PerfTest {
    private static final String ECS_CONFIG_PATH = USER_DIR + SEP + "config" + SEP + "server-info";

    private static final int OPS_PER_CLIENT = 100;

    private EnronDataset enronDataset;
    private ExternalConfigurationService ecs;
    private ReportBuilder reportBuilder;
    List<Performance> perfResults;

    private void init() throws IOException {
        ecs = new ExternalConfigurationService(ECS_CONFIG_PATH);
        reportBuilder = new ReportBuilder();
        enronDataset = new EnronDataset();
        enronDataset.loadData(500);
    }


    private void runTest(int numClients, int opsPerClient,
                         int numServers, int cacheSize, String strategy) throws InterruptedException {

        ClientRunner[] clientRunners = new ClientRunner[numClients];
        ecs.initService(numServers, cacheSize, strategy);
        ecs.startService();

        Status[] opTypes = new Status[]{Status.PUT, Status.GET};
        reportBuilder.blankLine();
        reportBuilder.lineSeparator();
//        reportBuilder.insert("number_of_servers: " + numServers);
//        reportBuilder.insert("number_of_clients: " + numClients);
        reportBuilder.insert("strategy: " + strategy);
        reportBuilder.insert("cache_size: " + cacheSize);
        reportBuilder.lineSeparator();

        for (Status opType : opTypes) {
            for (int i = 0; i < numClients; i++) {
                int n = ThreadLocalRandom.current().nextInt(ecs.getChord().size());
                KVServer kvServer = ecs.getChord().nodes().get(n);

                Client client = new Client(kvServer.getHost(), kvServer.getServicePort());
                clientRunners[i] = new ClientRunner(client, enronDataset, opType, opsPerClient);
            }

            Thread[] threads = new Thread[numClients];
            for (int i = 0; i < numClients; i++) {
                ClientRunner clientRunner = clientRunners[i];
                threads[i] = new Thread(clientRunner);
            }

            for (Thread t : threads)
                t.start();

            for (Thread t : threads)
                t.join();

            Thread.sleep(2000);
            perfResults = Arrays.stream(clientRunners).map(ClientRunner::getPerf).collect(Collectors.toList());
            reportBuilder.insert("op_type: " + opType.name());
            reportBuilder.insert("run_time (s): " + Arrays.toString(perfResults.stream().map(Performance::getRuntime).toArray(Double[]::new)));
            Double[] latencies = perfResults.stream().map(Performance::getLatency).toArray(Double[]::new);
            reportBuilder.insert("latencies (s/ops): " + Arrays.toString(latencies));
            Double[] throughputs = perfResults.stream().map(Performance::getThroughput).toArray(Double[]::new);
            reportBuilder.insert("throughputs (ops/s): " + Arrays.toString(throughputs));
            reportBuilder.insert("average_latency: " + Arrays.stream(latencies).mapToDouble(l -> l).average());
            reportBuilder.insert("average_throughput: " + Arrays.stream(throughputs).mapToDouble(tp -> tp).average());
            reportBuilder.blankLine();
        }
        ecs.shutDown();
        Thread.sleep(5000);
    }

//    private double averageThrougput(ClientRunner[] clientRunners) {
//        double sum = 0;
//        for (ClientRunner clientRunner : clientRunners) {
//            sum += clientRunner.getThroughput();
//        }
//        return sum / clientRunners.length;
//    }


    @Test
    public void test_multiple_clients_servers() {
        final int CACHE_SIZE = 1000;
        final String STRATEGY = "FIFO";

        final int[] numClients = new int[]{5};
        final int[] numServers = new int[]{5};
//        final int[] numClients = new int[]{1};
//        final int[] numServers = new int[]{1};

        try {
            init();
            for (int numClient : numClients) {
                for (int numServer : numServers) {
                    reportBuilder.insert("number_loaded_mails: " + enronDataset.getDataLoaded().size());
                    reportBuilder.insert("cache_size: " + CACHE_SIZE);
                    reportBuilder.insert("strategy: " + STRATEGY);
                    reportBuilder.insert("ops_per_client: " + OPS_PER_CLIENT);

                    runTest(numClient, OPS_PER_CLIENT, numServer, CACHE_SIZE, STRATEGY);
                }
            }
            reportBuilder.blankLine();

            Path perfDir = Paths.get(FileUtils.USER_DIR + SEP + "perf");
            if (!FileUtils.dirExists(perfDir))
                Files.createDirectories(perfDir);

            reportBuilder.save(Paths.get(perfDir.toString() + SEP + "multiple_clients_servers_1000_LFU.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void test_cacheSizes_and_strategies() {
        final int numClients = 5; // 5
        final int numServers = 1; // 1

        try {
            init();
            reportBuilder.insert("number_loaded_mails: " + enronDataset.getDataLoaded().size());
            reportBuilder.insert("num_clients: " + numClients);
            reportBuilder.insert("num_servers: " + numServers);
            reportBuilder.insert("ops_per_client: " + OPS_PER_CLIENT);

            Integer[] cacheSizes = new Integer[]{1, 100, 500, 1000, 2000, 5000};
            String[] strategies = {"FIFO", "LFU", "LRU"};
//                Integer[] cacheSizes = new Integer[]{1000};
//                String[] strategies = {"FIFO"};

            for (String strategy : strategies) {
                for (int cacheSize : cacheSizes) {

                    runTest(numClients, OPS_PER_CLIENT, numServers, cacheSize, strategy);
                }
            }
            reportBuilder.blankLine();

            Path perfDir = Paths.get(FileUtils.USER_DIR + SEP + "perf");
            if (!FileUtils.dirExists(perfDir))
                Files.createDirectories(perfDir);

            reportBuilder.save(Paths.get(perfDir.toString() + SEP + "cachesizes_strategies.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
