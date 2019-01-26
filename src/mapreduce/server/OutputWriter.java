package mapreduce.server;

import client.api.Client;
import ecs.NodeInfo;

import java.io.IOException;
import java.util.Map;

public class OutputWriter<KT, VT> {
    private Map<KT, VT> output;
    private Client client;

    public OutputWriter(Map<KT, VT> output, NodeInfo node) {
        this.output = output;
        this.client = new Client(node.getHost(), node.getPort());
    }

    public void write() throws IOException {
        for(Map.Entry<KT, VT> entry: output.entrySet()) {
            String key = String.valueOf(entry.getKey());
            String val = String.valueOf(entry.getValue());

            if (!client.isConnected())
                client.connect();

            client.put(key, val);
        }
    }


}
