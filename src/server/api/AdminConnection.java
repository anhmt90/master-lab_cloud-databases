package server.api;

import management.ConfigMessage;
import management.ConfigMessageMarshaller;
import management.ConfigStatus;
import management.IExternalConfigurationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.IMessage;
import protocol.MessageMarshaller;
import server.app.Server;
import util.StringUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

public class AdminConnection {
    private static final int MAX_MESSAGE_LENGTH = 2 + 20 + 1024 * 120;
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private boolean isOpen;

    private Socket ecsSocket;
    private Server server;
    private BufferedInputStream input;
    private BufferedOutputStream output;


    public AdminConnection(Socket ecsSocket, Server server) {
        this.ecsSocket = ecsSocket;
        this.server = server;
        isOpen = true;
    }

    /**
     * Initializes and starts the connection with ECS.
     * Loops until the connection is closed or aborted by the ECS.
     */
    public void poll() {
        try {
            output = new BufferedOutputStream(ecsSocket.getOutputStream());
            input = new BufferedInputStream(ecsSocket.getInputStream());

            send("Connection to KV Storage Server established: "
                    + ecsSocket.getLocalAddress() + " / "
                    + ecsSocket.getLocalPort());

            while (isOpen) {
                try {
                    ConfigMessage configMessage = receive();
                    handleAdminRequest(configMessage);

//                    send(handleRequest(kvMessage));

                    /* connection either terminated by the client or lost due to
                     * network problems*/
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

    private boolean handleAdminRequest(ConfigMessage configMessage) {
        switch (configMessage.getStatus()) {
            case STOP:
                return server.stopService();
            case START:
                return server.startService();
            case SHUTDOWN:
                   return server.shutdown();
            default:
                throw new IllegalArgumentException("Invalid request from ECS!");
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     *
     * @param text the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void send(String text) throws IOException {
        byte[] messageBytes = StringUtils.toByteArray(text);
        writeOutput(text, messageBytes);
    }

    /**
     * Send a marshalled message out through the server socket
     *
     * @param object       Message in String format for logging
     * @param messageBytes The marshalled message
     * @throws IOException
     */
    private void writeOutput(String object, byte[] messageBytes) throws IOException {
        output.write(messageBytes);
        output.flush();
        LOG.info("SEND \t<"
                + ecsSocket.getInetAddress().getHostAddress() + ":"
                + ecsSocket.getPort() + ">: '"
                + object + "'");
    }

    /**
     * Receives a message sent by a client
     *
     * @return the received message
     * @throws IOException
     */
    private ConfigMessage receive() throws IOException {
        int index = 0;
        byte[] messageBytes = new byte[MAX_MESSAGE_LENGTH];

        int bytesCopied = input.read(messageBytes);
        LOG.info("Read " + bytesCopied + " from input stream");

        ConfigMessage message = ConfigMessageMarshaller.unmarshall(messageBytes);

        LOG.info("RECEIVE \t<"
                + ecsSocket.getInetAddress().getHostAddress() + ":"
                + ecsSocket.getPort() + ">: '"
                + message.toString().trim() + "'");

        return message;
    }

}
