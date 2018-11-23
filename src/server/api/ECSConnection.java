package server.api;

import management.ConfigMessage;
import management.ConfigMessageMarshaller;
import management.ConfigStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.app.Server;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ECSConnection {
    private static final int MAX_MESSAGE_LENGTH = 2 + 20 + 1024 * 120;
    private static final int ECS_PORT = 54321;

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private boolean isOpen;

    private Socket ecsSocket;
    private Server server;
    private BufferedInputStream input;
    private BufferedOutputStream output;


    public ECSConnection(String ecsAddress, Server server) {
        this.server = server;
        initAdminTunnel(ecsAddress);
    }

    /**
     * Initializes and starts the connection with ECS.
     * Loops until the connection is closed or aborted by the ECS.
     */
    public void pollRequests() {
        try {
            output = new BufferedOutputStream(ecsSocket.getOutputStream());
            input = new BufferedInputStream(ecsSocket.getInputStream());

            while (isOpen) {
                try {
                    ConfigMessage configMessage = poll();

                    boolean success = handleAdminRequest(configMessage);
                    ConfigMessage ack = new ConfigMessage(getAckStatus(configMessage.getStatus(), success));
                    send(ack);

                } catch (IOException ioe) {
                    LOG.error("Error! Connection lost!");
                    isOpen = false;
                }
            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection could not be established!", ioe);

        } finally {
            try {
                if (ecsSocket != null) {
                    input.close();
                    output.close();
                    ecsSocket.close();
                }
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private ConfigStatus getAckStatus(ConfigStatus reqStatus, boolean success) {
        switch (reqStatus) {
            case INIT:
                return success ? ConfigStatus.INIT_SUCCESS : ConfigStatus.INIT_ERROR;
            case START:
                return success ? ConfigStatus.START_SUCCESS : ConfigStatus.START_ERROR;
            case LOCK_WRITE:
                return success ? ConfigStatus.LOCK_WRITE_SUCCESS : ConfigStatus.LOCK_WRITE_ERROR;
            case UNLOCK_WRITE:
                return success ? ConfigStatus.UNLOCK_WRITE_SUCCESS : ConfigStatus.UNLOCK_WRITE_ERROR;
            case UPDATE_METADATA:
                return success ? ConfigStatus.UPDATE_METADATA_SUCCESS : ConfigStatus.UPDATE_METADATA_ERROR;
            case MOVE_DATA:
                return success ? ConfigStatus.MOVE_DATA_SUCCESS : ConfigStatus.MOVE_DATA_ERROR;
            case STOP:
                return success ? ConfigStatus.STOP_SUCCESS : ConfigStatus.STOP_ERROR;
                default:
                    throw new IllegalStateException("Unknown status of request!");
        }
    }

    private boolean handleAdminRequest(ConfigMessage configMessage) {
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
                return server.moveData(configMessage.getKeyRange() ,configMessage.getTargetServer());
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
        output.write(ConfigMessageMarshaller.marshall(message));
        output.flush();
        LOG.info("SEND \t<"
                + ecsSocket.getInetAddress().getHostAddress() + ":"
                + ecsSocket.getPort() + ">: '"
                + message.toString() + "'");

    }

    /**
     * Receives a message sent by a client
     *
     * @return the received message
     * @throws IOException
     */
    private ConfigMessage poll() throws IOException {
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];

        int bytesCopied = input.read(messageBuffer);
        LOG.info("Read " + bytesCopied + " from input stream");

        ConfigMessage message = ConfigMessageMarshaller.unmarshall(messageBuffer);

        LOG.info("RECEIVE \t<"
                + ecsSocket.getInetAddress().getHostAddress() + ":"
                + ecsSocket.getPort() + ">: '"
                + message.toString().trim() + "'");

        return message;
    }

    private void initAdminTunnel(String ecsAddress) {
        LOG.info("Initialize admin tunnel  ...");
        ecsSocket = new Socket();
        try {
            ecsSocket.connect(new InetSocketAddress(ecsAddress, ECS_PORT), 5000);
            isOpen = true;
            send(new ConfigMessage(ConfigStatus.BOOT_SUCCESS));

            LOG.info("Server connects to ECS on port: " + ecsSocket.getLocalPort());
        } catch (IOException e) {
            LOG.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                LOG.error("Port " + ECS_PORT + " is already bound!");
            }
            e.printStackTrace();
        }
    }

    public boolean isOpen() {
        return isOpen && ecsSocket != null && !ecsSocket.isClosed() && !ecsSocket.isConnected();
    }

}
