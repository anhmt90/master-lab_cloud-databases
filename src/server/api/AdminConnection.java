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
import java.net.ServerSocket;
import java.net.Socket;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;

public class AdminConnection implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private static final int BOOT_ACK_PORT = 54321;
    private boolean isOpen;

    private Socket ecsSocket;

    private Server server;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;


    public AdminConnection(Server server, Socket socket) {
        this.server = server;
        this.ecsSocket = socket;
    }

    /**
     * Initializes and starts the connection with ECS.
     * Loops until the connection is closed or aborted by the ECS.
     */
    public void pollRequests() {
        try {
            bos = new BufferedOutputStream(ecsSocket.getOutputStream());
            bis = new BufferedInputStream(ecsSocket.getInputStream());

            while (server.isRunning()) {
                ConfigMessage configMessage = poll();

                boolean success = handleAdminRequest(configMessage);
                ConfigMessage ack = new ConfigMessage(getAckStatus(configMessage.getStatus(), success));
                send(ack);

            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection could not be established!", ioe);
        } finally {
            try {
                if (ecsSocket != null) {
                    bis.close();
                    bos.close();
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
                return success ? ConfigStatus.INIT_SUCCESS : ConfigStatus.ERROR;
            case START:
                return success ? ConfigStatus.START_SUCCESS : ConfigStatus.ERROR;
            case LOCK_WRITE:
                return success ? ConfigStatus.LOCK_WRITE_SUCCESS : ConfigStatus.ERROR;
            case UNLOCK_WRITE:
                return success ? ConfigStatus.UNLOCK_WRITE_SUCCESS : ConfigStatus.ERROR;
            case UPDATE_METADATA:
                return success ? ConfigStatus.UPDATE_METADATA_SUCCESS : ConfigStatus.ERROR;
            case MOVE_DATA:
                return success ? ConfigStatus.MOVE_DATA_SUCCESS : ConfigStatus.ERROR;
            case STOP:
                return success ? ConfigStatus.STOP_SUCCESS : ConfigStatus.ERROR;
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
        bos.write(ConfigMessageMarshaller.marshall(message));
        bos.flush();
        LOG.info("SEND \t<"
            + ecsSocket.getInetAddress().getHostAddress() + ":"
            + ecsSocket.getPort() + ">: '"
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

        int bytesCopied = bis.read(messageBuffer);
        LOG.info("Read " + bytesCopied + " from input stream");

        ConfigMessage message = ConfigMessageMarshaller.unmarshall(messageBuffer);

        LOG.info("RECEIVE \t<"
            + ecsSocket.getInetAddress().getHostAddress() + ":"
            + ecsSocket.getPort() + ">: '"
            + message.toString().trim() + "'");

        return message;
    }

    @Override
    public void run() {

    }
}
