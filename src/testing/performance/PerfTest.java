package testing.performance;

import client.api.Client;
import ecs.ExternalConfigurationService;
import ecs.KVServer;
import org.junit.Test;
import protocol.IMessage.Status;
import util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static util.FileUtils.SEP;
import static util.FileUtils.USER_DIR;

public class PerfTest {
    private static final String ENRON_DATASET = USER_DIR + "/../enron_mail_20150507/maildir";
    private static final String ECS_CONFIG_PATH = USER_DIR + SEP + "config" + SEP + "server-info";

    private static final int[] CLIENT_NUMBERS = new int[]{1, 5, 20};
    private static final int[] SERVER_NUMBERS = new int[]{1, 5, 10};

    private static final int OPS_PER_CLIENT = 1000;

    private EnronDataset enronDataset;
    private ExternalConfigurationService ecs;
    private ReportBuilder reportBuilder;
    List<Performance> perfResults;

    private void init() throws IOException {
        ecs = new ExternalConfigurationService(ECS_CONFIG_PATH);
        reportBuilder = new ReportBuilder();
        enronDataset = new EnronDataset(ENRON_DATASET);
        enronDataset.loadData(3000);
    }


    private List<Performance> runTest(int numClients, Status opType, int opsPerClient,
                                      int numServers, int cacheSize, String strategy) throws Exception {

        ClientRunner[] clientRunners = new ClientRunner[numClients];
        ecs.initService(numServers, cacheSize, strategy);
        ecs.startService();
        KVServer kvServer = ecs.getChord().nodes().iterator().next();

        for (int i = 0; i < numClients; i++) {
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

        ecs.shutDown();
        return Arrays.stream(clientRunners).map(ClientRunner::getPerf).collect(Collectors.toList());
    }

//    private double averageThrougput(ClientRunner[] clientRunners) {
//        double sum = 0;
//        for (ClientRunner clientRunner : clientRunners) {
//            sum += clientRunner.getThroughput();
//        }
//        return sum / clientRunners.length;
//    }

    @Test
    public void test_cacheSizes_and_strategies() {
        final int numClients = 5; // 5
        final int numServers = 1; // 1

        try {
            init();
            Status[] opTypes = new Status[]{Status.PUT, Status.GET};
            for (Status opType : opTypes) {
                reportBuilder.appendToLine("num_clients");
                reportBuilder.appendToLine("ops_per_client");
                reportBuilder.appendToLine("op_type");
                reportBuilder.startNewLine();

                reportBuilder.appendToLine(numClients);
                reportBuilder.appendToLine(OPS_PER_CLIENT);
                reportBuilder.appendToLine(opType.name());
                reportBuilder.startNewLine();

//                Integer[] cacheSizes = new Integer[]{1, 100, 500, 1000, 2000, 5000};
//                String[] strategies = {"FIFO", "LFU", "LRU"};
                Integer[] cacheSizes = new Integer[]{1000};
                String[] strategies = {"FIFO"};

                for (String strategy : strategies) {
                    reportBuilder.addHeader(strategy);

                    for (int i = 0; i < cacheSizes.length; i++) {
                        int cacheSize = cacheSizes[i];
                        perfResults = runTest(numClients, opType, OPS_PER_CLIENT, numServers, cacheSize, strategy);
                    }

                    reportBuilder.addHeader("cache_size");
                    reportBuilder.addNewLineWith(cacheSizes);
                    reportBuilder.addHeader("elapsed_time");
                    reportBuilder.addNewLineWith(perfResults.stream().map(Performance::getRuntime).toArray(Double[]::new));
                    reportBuilder.addHeader("latency");
                    reportBuilder.addNewLineWith(perfResults.stream().map(Performance::getLatency).toArray(Double[]::new));
                    reportBuilder.addHeader("throughput");
                    reportBuilder.addNewLineWith(perfResults.stream().map(Performance::getThroughput).toArray(Double[]::new));
                }
                Path perfDir = Paths.get(FileUtils.USER_DIR + SEP + "perf");
                if (!FileUtils.dirExists(perfDir))
                    Files.createDirectories(perfDir);

                reportBuilder.writeToFile(Paths.get(perfDir.toString() + SEP + "cachesizes_strategies.txt"));
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


//    @Test
//    public void test_multiple_clients_servers() throws Exception {
//
//        final int CACHE_SIZE = 100;
//        final String STRATEGY = "LFU";
//
//        init();
//        for (int numClient : CLIENT_NUMBERS) {
//            reportBuilder.addHeader("clients_number");
//            reportBuilder.appendToLine(numClient);
//            reportBuilder.startNewLine();
//            long[] times = new long[SERVER_NUMBERS.length];
//            long[] minLatencies = new long[SERVER_NUMBERS.length];
//            long[] maxLatencies = new long[SERVER_NUMBERS.length];
//            long[] opsPerSec = new long[SERVER_NUMBERS.length];
//
//            for (int i = 0; i < SERVER_NUMBERS.length; i++) {
//                int numServer = CLIENT_NUMBERS[i];
//                long totalOps = numClient * OPS_PER_CLIENT;
//                long[] results = runTest(numClient, PUT_RATIO, OPS_PER_CLIENT, numServer, CACHE_SIZE, STRATEGY);
//                times[i] = results[0];
//                minLatencies[i] = results[1];
//                maxLatencies[i] = results[2];
//                double seconds = (double) results[0] / 1_000_000_000.0;
//                long opps = (long) (seconds / totalOps);
//                opsPerSec[i] = opps;
//            }
//            reportBuilder.addHeader("servers_number");
//            reportBuilder.addNewLineWith(SERVER_NUMBERS);
//            reportBuilder.addHeader("elapsed_time");
//            reportBuilder.addNewLineWith(times);
//            reportBuilder.addHeader("min_latency");
//            reportBuilder.addNewLineWith(minLatencies);
//            reportBuilder.addHeader("max_latency");
//            reportBuilder.addNewLineWith(maxLatencies);
//            reportBuilder.addHeader("ops_per_second");
//            reportBuilder.addNewLineWith(opsPerSec);
//        }
//        String outputPath = FileUtils.WORKING_DIR + SEP + "performance_results" + SEP + "servers_clients.txt";
//        reportBuilder.writeToFile(outputPath);
//    }

}
