package mapreduce.server;

import client.api.Client;
import ecs.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.IMessage;
import protocol.kv.Message;
import protocol.mapreduce.Utils;
import server.app.Server;

import java.io.IOException;
import java.security.acl.LastOwnerException;
import java.util.Map;
import java.util.Set;


public class OutputWriter<KT, VT> {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private Map<KT, VT> output;
    private Client client;
    private TaskHandler taskHandler;

    public OutputWriter(MapReduce mapper, TaskHandler taskHandler) {
        output = mapper.getOutput();
        this.taskHandler = taskHandler;
        NodeInfo defaultKnownNode = taskHandler.getFirstNodeFromMetadata();
        client = new Client(defaultKnownNode.getHost(), defaultKnownNode.getPort());
    }

    public void write() {
        try {
            for (Map.Entry<KT, VT> entry : output.entrySet()) {
                String key = String.valueOf(entry.getKey());
                String val = String.valueOf(entry.getValue());

                if (!client.isConnected()) {
                    client.connect();
                }
                IMessage message = Message.createPUTMessage(key, val);
                message.setMRToken(createToken());
                client.put(message);
            }
        } catch (IOException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
    }

    private String createToken() {
        return taskHandler.getCurrJobId() + Utils.JOBID_NODEID_SEP + taskHandler.getNodeId();
    }
}
