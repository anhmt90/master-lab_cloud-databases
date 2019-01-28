package mapreduce.server;

import client.api.Client;
import ecs.NodeInfo;
import mapreduce.common.TaskType;
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
        client.setMetadata(taskHandler.getMetadata());
    }

    public void write() {
        try {
            for (Map.Entry<KT, VT> entry : output.entrySet()) {
                String word = String.valueOf(entry.getKey());
                String number = String.valueOf(entry.getValue());

                if (!client.isConnected()) {
                    client.connect();
                }
                IMessage message = Message.createPUTMessage(word, number);
                message.setMRToken(createToken());

                LOG.info("Writing MR output: " + message);
                client.put(message);
            }
        } catch (IOException e) {
            LOG.error(e);
        } catch (RuntimeException e) {
            LOG.error(e);
            throw new RuntimeException(e);
        }
        LOG.info("Writing MR output ended successfully");
    }

    private String createToken() {
        String token = taskHandler.getCurrJobId();
        if (taskHandler.getTaskType().equals(TaskType.MAP))
            token += Utils.JOBID_NODEID_SEP + taskHandler.getNodeId();
        return token;
    }
}
