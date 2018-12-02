package server.api;

import management.ConfigMessage;
import management.ConfigStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.app.Server;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class InternalConnection implements Runnable {

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private final InternalConnectionManager manager;
    private boolean isOpen;

    private Socket peer;

    private Server server;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;

    public InternalConnection(InternalConnectionManager manager, Socket peer, Server server) {
        this.manager = manager;
        this.server = server;
        this.peer = peer;
        isOpen = true;
    }

    /**
     * Listens to the admin instructions from ECS
     * Loops until the connection is closed or aborted by the ECS.
     */
    @Override
    public void run() {
        try {
            while (isOpen && server.isRunning()) {
                ConfigMessage configMessage = poll();
                boolean success = (!configMessage.getStatus().equals(ConfigStatus.HEART_BEAT))
                        ? handleRequest(configMessage) : true;

                ConfigMessage ack = new ConfigMessage(getAckStatus(configMessage.getStatus(), success));
                LOG.info("sending ACK " + ack.getStatus());
                send(ack);

            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection lost", ioe);
            isOpen = false;
        } finally {
            try {
                if (peer != null) {
                    ois.close();
                    oos.close();
                    peer.close();
                }
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    /**
     * check the result of handling admin requests and get an appropriate ACK status
     *
     * @param reqStatus admin request status
     * @param success   result of handling admin requests, true or fals
     * @return
     */
    private ConfigStatus getAckStatus(ConfigStatus reqStatus, boolean success) {
        if(!success)
            return ConfigStatus.ERROR;
        switch (reqStatus) {
            case INIT:
                return ConfigStatus.INIT_SUCCESS;
            case START:
                return ConfigStatus.START_SUCCESS;
            case LOCK_WRITE:
                return ConfigStatus.LOCK_WRITE_SUCCESS;
            case UNLOCK_WRITE:
                return ConfigStatus.UNLOCK_WRITE_SUCCESS;
            case UPDATE_METADATA:
                return ConfigStatus.UPDATE_METADATA_SUCCESS;
            case MOVE_DATA:
                return ConfigStatus.MOVE_DATA_SUCCESS;
            case STOP:
                return ConfigStatus.STOP_SUCCESS;
            case SHUTDOWN:
                return ConfigStatus.SHUTDOWN_SUCCESS;
            case HEART_BEAT:
                return ConfigStatus.ALIVE;
            default:
                throw new IllegalStateException("Unknown status of request!");
        }
    }

    /**
     * calls relevant functions based on the admin requests
     *
     * @param configMessage the message containing admin quests
     * @return
     */
    private boolean handleRequest(ConfigMessage configMessage) {
        LOG.info("handle request from ECS with status " + configMessage.getStatus());
        switch (configMessage.getStatus()) {
            case INIT:
                return server.initKVServer(configMessage.getMetadata(), configMessage.getCacheSize(), configMessage.getStrategy());
            case STOP:
                return server.stopService();
            case START:
                return server.startService();
            case LOCK_WRITE:
                return server.lockWrite();
            case UNLOCK_WRITE:
                return server.unlockWrite();
            case UPDATE_METADATA:
                return server.update(configMessage.getMetadata());
            case MOVE_DATA:
                return server.moveData(configMessage.getKeyRange(), configMessage.getTargetServer());
            case SHUTDOWN:
                return server.shutdown();
            default:
                throw new IllegalStateException("Unknown admin request!");
        }
    }


    /**
     * Sends out a config message
     *
     * @param message Message that is sent
     * @throws IOException
     */
    public void send(ConfigMessage message) throws IOException {
        LOG.info("sending message to " + peer.getInetAddress() + ":" + peer.getPort());
        oos = new ObjectOutputStream(peer.getOutputStream());
        oos.writeObject(message);
        oos.flush();
        LOG.info("SEND \t<"
                + peer.getInetAddress().getHostAddress() + ":"
                + peer.getPort() + ">: '"
                + message.toString() + "'");

    }

    /**
     * Receives a message sent by ECS
     *
     * @return the received message
     * @throws IOException
     */
    private ConfigMessage poll() throws IOException {
        LOG.info("polling message from " + peer.getInetAddress() + ":" + peer.getPort());
        ois = new ObjectInputStream(peer.getInputStream());
        ConfigMessage message = null;
        try {
            message = (ConfigMessage) ois.readObject();
        } catch (ClassNotFoundException e) {
            LOG.error(e);
        }

        LOG.info("RECEIVE \t<"
                + peer.getInetAddress().getHostAddress() + ":"
                + peer.getPort() + ">: '"
                + message.toString().trim() + "'");

        return message;
    }

    public void setOpen(boolean open) {
        isOpen = open;
    }

    public boolean isOpen() {
        return isOpen;
    }
}
