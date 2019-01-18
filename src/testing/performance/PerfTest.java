package testing.performance;

import client.api.Client;
import ecs.ExternalConfigurationService;
import ecs.KVServer;
import org.junit.Test;
import protocol.kv.IMessage.Status;
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
        enronDataset.loadData(200);
    }


    private void runTest(int numClients, int opsPerClient,
                         int numServers, int cacheSize, String strategy) throws InterruptedException {

        ecs.initService(numServers, cacheSize, strategy);
        ecs.startService();

        Status[] opTypes = new Status[]{Status.PUT, Status.GET};
        reportBuilder.blankLine();
        reportBuilder.lineSeparator();
        reportBuilder.insert("number_of_servers: " + numServers);
        reportBuilder.insert("number_of_clients: " + numClients);
//        reportBuilder.insert("strategy: " + strategy);
//        reportBuilder.insert("cache_size: " + cacheSize);
        reportBuilder.lineSeparator();

        for (Status opType : opTypes) {
            create_and_run_clients(numClients, opsPerClient, opType);

            reportBuilder.insert("op_type: " + opType.name());
            reportBuilder.insert("run_time (s): " + Arrays.toString(perfResults.stream().map(Performance::getRuntime).toArray(Double[]::new)));

            Double[] latencies = perfResults.stream().map(Performance::getLatency).toArray(Double[]::new);
            reportBuilder.insert("latencies (s/ops): " + Arrays.toString(latencies));

            Double[] throughputs = perfResults.stream().map(Performance::getThroughput).toArray(Double[]::new);
            reportBuilder.insert("throughputs (ops/s): " + Arrays.toString(throughputs));

            reportBuilder.insert("average_latency: " + Arrays.stream(latencies).mapToDouble(l -> l).average().getAsDouble());
            reportBuilder.insert("average_throughput: " + Arrays.stream(throughputs).mapToDouble(tp -> tp).average().getAsDouble());
            reportBuilder.blankLine();
        }
        ecs.shutdown();
        Thread.sleep(5000);
    }

    private void create_and_run_clients(int numClients, int opsPerClient, Status opType) throws InterruptedException {
        ClientRunner[] clientRunners = new ClientRunner[numClients];
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

        Thread.sleep(5000);
        perfResults = Arrays.stream(clientRunners).map(ClientRunner::getPerf).collect(Collectors.toList());
    }

    @Test
    public void test_multiple_clients_servers() {
        final int CACHE_SIZE = 1000;
        final String STRATEGY = "FIFO";

        final int numClient = 5;
        final int[] numServers = new int[]{3};
//        final int[] numClients = new int[]{10};
//        final int[] numServers = new int[]{2, 5};

        try {
            init();
            reportBuilder.insert("number_loaded_mails: " + enronDataset.getDataLoaded().size());
            reportBuilder.insert("cache_size: " + CACHE_SIZE);
            reportBuilder.insert("strategy: " + STRATEGY);
            reportBuilder.insert("ops_per_client: " + OPS_PER_CLIENT);


            for (int numServer : numServers) {

                runTest(numClient, OPS_PER_CLIENT, numServer, CACHE_SIZE, STRATEGY);
            }
            reportBuilder.blankLine();

            Path perfDir = Paths.get(FileUtils.USER_DIR + SEP + "perf");
            if (!FileUtils.dirExists(perfDir))
                Files.createDirectories(perfDir);

            reportBuilder.save(Paths.get(perfDir.toString() + SEP + "multiple_clients_servers_" + CACHE_SIZE + "_" + STRATEGY + "_" + numClient + "clients.txt"));
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

//            Integer[] cacheSizes = new Integer[]{1, 100, 500, 1000, 2000, 5000};
//            String[] strategies = {"FIFO", "LFU", "LRU"};
            Integer[] cacheSizes = new Integer[]{1000};
            String[] strategies = {"FIFO"};

            for (String strategy : strategies) {
                for (int cacheSize : cacheSizes) {

                    runTest(numClients, OPS_PER_CLIENT, numServers, cacheSize, strategy);
                }
            }
            reportBuilder.blankLine();

            saveReport("cachesizes_strategies" + numServers + "_" + numClients);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test_scaling_ring() throws InterruptedException, IOException {
        final int CACHE_SIZE = 1000;
        final String STRATEGY = "FIFO";

        final int numClients = 3;
        final int numServers = 3;
        final int numServersToScale = 2;

        init();
        ecs.initService(numServers, CACHE_SIZE, STRATEGY);
        ecs.startService();

        create_and_run_clients(numClients, OPS_PER_CLIENT, Status.PUT);
        perfResults.clear();

        reportBuilder.insert("number_loaded_mails: " + enronDataset.getDataLoaded().size());
        reportBuilder.insert("clients: " + numClients);
        reportBuilder.insert("num_PUTs_per_client: " + OPS_PER_CLIENT);
        reportBuilder.insert("initial_num_servers: " + numServers);
        reportBuilder.insert("num_servers_to_scale: " + numServersToScale);
        reportBuilder.lineSeparator();

        final String UP = "UP";
        final String DOWN = "DOWN";
        for (final String scale : new String[]{UP, DOWN}) {
            reportBuilder.insert("scaling_" + scale);
            for (int i = 0; i < numServersToScale; i++) {
                System.out.println("=============================> " + scale + " " + i);
                Stopwatch sw = new Stopwatch();
                sw.tick();
                switch (scale) {
                    case UP:
                        ecs.addNode(CACHE_SIZE, STRATEGY);
                        break;
                    case DOWN:
                        ecs.removeNode();
                        break;
                }
                sw.tock();
                perfResults.add(new Performance().withRuntime(sw.getRuntimeInSeconds()));
                Thread.sleep(3000);
            }

            Double[] runtimes = perfResults.stream().map(Performance::getRuntime).toArray(Double[]::new);
            reportBuilder.insert("run_time (s): " + Arrays.toString(runtimes));
            reportBuilder.insert("average_scale_" + scale + "_time: " + Arrays.stream(runtimes).mapToDouble(rt -> rt).average().getAsDouble());
            reportBuilder.blankLine();
            perfResults.clear();
        }
        saveReport("scale_" + numServersToScale + "_with_initial_" + numServers + "servers");

        ecs.shutdown();
        Thread.sleep(2000);
    }

    private void saveReport(String reportName) throws IOException {
        Path perfDir = Paths.get(FileUtils.USER_DIR + SEP + "perf");
        if (!FileUtils.dirExists(perfDir))
            Files.createDirectories(perfDir);
        reportBuilder.save(Paths.get(perfDir.toString() + SEP + reportName + ".txt"));
    }

}
