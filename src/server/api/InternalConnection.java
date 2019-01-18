package server.api;

import management.ConfigMessage;
import management.MessageSerializer;
import management.ConfigStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.app.Server;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;

import static protocol.kv.IMessage.MAX_MESSAGE_LENGTH;

/**
 * A stateful connection from ECS to the server. This is created by {@link InternalConnectionManager}
 * on the server to handle requests from the ECS
 */
public class InternalConnection implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private static final int MAX_ALLOWED_EOF = 3;
    private final InternalConnectionManager manager;
    private boolean isOpen;

    private Socket peer;

    private Server server;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    ConfigMessage configMessage;

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
        int eofCounter = 0;
        try {
            while (isOpen && server.isRunning()) {
                configMessage = poll();

                if (configMessage == null) {
                    eofCounter++;
                    if (eofCounter >= MAX_ALLOWED_EOF) {
                        LOG.warn("Got " + eofCounter + " successive EOF signals! Assume the other end has terminated but not closed the socket properly. " +
                                "Tear down connection now");
                        isOpen = false;
                    }
                    continue;
                }

                boolean success = handleRequest(configMessage);

                ConfigMessage ack = new ConfigMessage(getAckStatus(configMessage.getStatus(), success));
                LOG.info("sending ACK " + ack.getStatus());
                send(ack);

            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection lost", ioe);
            isOpen = false;
        } catch (Exception e) {
            LOG.error("Runtime exception!", e);
        } finally {
            try {
                close();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    void close() throws IOException {
        if (configMessage.getStatus().equals(ConfigStatus.SHUTDOWN))
            return;
        boolean success = false;
        if (manager != null)
            success = manager.getConnectionTable().remove(this);
        LOG.info("removed from internalConnectionTable?" + success + ". Is Manager destructed? " + (manager == null) + ".\nClosing socket now.");
        if (peer != null) {
            bis.close();
            bos.close();
            peer.close();
            bis = null;
            bos = null;
            peer = null;
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
        if (!success)
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
                return server.moveData(configMessage.getTargetServer().getWriteRange(), configMessage.getTargetServer());
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
        bos = new BufferedOutputStream(peer.getOutputStream());
        byte[] bytes = MessageSerializer.serialize(message);
        bos.write(bytes);
        bos.flush();

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
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        while (true) {
            try {
                bis = new BufferedInputStream(peer.getInputStream());
                int justRead = bis.read(messageBuffer);
                if (justRead < 0)
                    return null;

                ConfigMessage message = MessageSerializer.deserialize(Arrays.copyOfRange(messageBuffer, 0, justRead));

                LOG.info("RECEIVE \t<"
                        + peer.getInetAddress().getHostAddress() + ":"
                        + peer.getPort() + ">: '"
                        + message.toString().trim() + "'");
                return message;
            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            }
        }
    }

    public void setOpen(boolean open) {
        isOpen = open;
    }

    public boolean isOpen() {
        return isOpen;
    }

    public ConfigMessage getConfigMessage() {
        return configMessage;
    }
}
