package testing.performance;

import client.api.Client;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.IMessage.Status;
import testing.AllTests;

import java.io.IOException;

class ClientRunner implements Runnable {
    private static Logger LOG = LogManager.getLogger(AllTests.TEST_LOG);

    private final Client client;
    private final Status opType;
    private final int ops;
    private final EnronDataset enronDataset;

    private Performance perf;
    int count = 0;

    public ClientRunner(Client client, EnronDataset enronDataset, Status opType, int ops) {

        if(!opType.equals(Status.PUT) && !opType.equals(Status.GET))
                throw new IllegalArgumentException("Not supported opType " + opType);

        this.enronDataset = enronDataset;
        this.client = client;
        this.opType = opType;
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
        System.out.println(opType.equals(Status.PUT) ? "putting...": "getting...");
        sw.tick();
        switch (opType) {
            case PUT:
                putAll();
                break;
            case GET:
                getAll();
                break;
        }
        sw.tock();
        client.disconnect();
        perf = new Performance()
                .withNumOps(ops)
                .withRuntime(sw.getRuntimeInSeconds());
    }


    private void getAll() {
        for (int i = 0; i < ops; i++) {
            EnronDataset.KV kvPair = enronDataset.getRandom();
            try {
                client.get(kvPair.key);
//                count++;
//                LOG.info("COUNT = " + count);
            } catch (IOException e) {
                LOG.error("Failed to get " + kvPair.key, e);
            }
        }
    }

    private void putAll() {
        for (int i = 0; i < ops; i++) {
            EnronDataset.KV kvPair = enronDataset.getRandom();
            try {
                client.put(kvPair.key, kvPair.val);
//                count++;
//                LOG.info("COUNT = " + count);
            } catch (IOException e) {
                LOG.error("Failed to put " + kvPair.key, e);
            }
        }
    }

    public Performance getPerf() {
        return perf;
    }
}
