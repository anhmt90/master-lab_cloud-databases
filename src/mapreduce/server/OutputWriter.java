package mapreduce.server;

import client.api.Client;
import ecs.NodeInfo;
import protocol.kv.K;
import protocol.kv.V;
import util.FileUtils;
import util.HashUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;

import static server.api.BatchDataTransferProcessor.getHashedKeyFromFileName;

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
