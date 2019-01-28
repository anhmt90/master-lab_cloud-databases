package mapreduce.client;

import client.api.Client;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.IMessage;
import protocol.kv.IMessage.Status;
import protocol.kv.K;
import protocol.kv.Message;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static mapreduce.client.Driver.MAPREDUCE_LOG;

public class OutputCollector {
    private static Logger LOG = LogManager.getLogger(MAPREDUCE_LOG);

    private Client client;
    private Driver driver;

    public OutputCollector(Client client, Driver driver) {
        this.client = client;
        this.driver = driver;
    }

    public void collect() {
        ConcurrentHashMap<String, String> outputs = new ConcurrentHashMap<>();
        LOG.error("Current outputs: " + Arrays.toString(outputs.entrySet().toArray()));

        Iterator<String> iter = driver.getKeys().iterator();
        LOG.error("Current key set: " + Arrays.toString(driver.getKeys().toArray()));

        while (iter.hasNext()) {
            String key = iter.next();
            try {
                IMessage message = new Message(Status.GET, new K(key));
                message.setMRToken(driver.getJobId());
                IMessage resp = client.get(message);
                if (resp.getStatus().equals(Status.GET_ERROR))
                    continue; //TODO can be done better than ignoring the error and later getting the imprecise final result
                String val = resp.getV().get();
                outputs.put(key, outputs.containsKey(key) ?  outputs.get(key)  + "\n" + val : val);

            } catch (IOException e) {
                LOG.error(e);
            }
        }
        driver.setOutputs(outputs);
    }
}
