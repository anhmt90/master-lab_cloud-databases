package server.api;

import client.api.Client;
import ecs.NodeInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.IMessage;
import server.app.Server;

import java.io.IOException;

/**
 * This class is responsible for replicating the PUT-messages to a replica
 */
public class Replicator implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    /**
     * the PUT-message to be replicated
     */
    private IMessage message;

    /**
     * client library to create connection to the replica
     */
    private Client client;

    /**
     * metadata of the replica in question
     */
    private NodeInfo replica;

    public Replicator(NodeInfo replica) {
        this.replica = replica;
        resetClient();
        createClient();
    }

    /**
     * prepares client to establish connection
     */
    private void createClient() {
        client = new Client(replica.getHost(), replica.getPort());
        client.setConnectedNode(replica);
    }

    /**
     * disconnects and nullifies the client
     */
    private void resetClient() {
        if (client != null && client.isConnected())
            client.disconnect();
        client = null;
    }

    @Override
    public void run() {
        try {
            if (client == null)
                createClient();

            if (!client.isConnected())
                client.connect();
            LOG.info("replicating  " + message);

            client.put(message);
        } catch (IOException e) {
            LOG.error(e);
        } finally {
            resetClient();
        }
    }

    public void setMessage(IMessage message) {
        this.message = message;
    }

    public NodeInfo getReplica() {
        return replica;
    }
}
