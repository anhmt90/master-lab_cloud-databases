package server.api;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import protocol.*;
import protocol.IMessage.Status;
import server.app.Server;
import server.storage.PUTStatus;
import server.storage.cache.CacheManager;
import util.LogUtils;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;

import java.io.*;
import java.net.Socket;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private boolean isOpen;

    private final Server server;
    private Socket clientSocket;
    private BufferedInputStream input;
    private BufferedOutputStream output;

    private CacheManager cm;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Server server, Socket clientSocket, CacheManager cm) {
        this.server = server;
        this.clientSocket = clientSocket;
        this.cm = cm;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        IMessage requestMessage = null;
        IMessage responseMessage;
        try {
            while (isOpen) {
                try {
                    requestMessage = receive();
                    responseMessage = handleRequest(requestMessage);
                    send(responseMessage);

                } catch (IllegalArgumentException iae) {
                    LOG.error("IllegalArgumentException", iae);
                    LOG.error(requestMessage.toString());
                    try {
                        send(new Message(Status.PUT_ERROR));
                    } catch (IOException ioe) {
                        LOG.error("Error! Connection lost!", ioe);
                    }
                    LOG.warn(iae);
                } catch (IOException ioe) {
                    LOG.error("Error! Connection lost!", ioe);
                    LOG.warn("Setting isOpen to false");
                    isOpen = false;
                } catch (Exception e) {
                    LOG.error("Exception", e);
                    e.printStackTrace();
                    isOpen = false;
                }
            }
        } finally {
            try {
                LOG.warn("CLOSING SOCKET...");
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }


    /**
     * Handle request sent from client
     *
     * @param message the request message from the client
     * @return a response message to the client
     */
    private IMessage handleRequest(IMessage message) {
        if (server.isStopped()) {
            LOG.info("Server is stopped");
            return new Message(Status.SERVER_STOPPED);
        }

        K key = message.getK();
        V val = message.getV();

        if (!server.getHashRange().inRange(key.getString())) {
            LOG.info("Server not responsible! Server hash range is " + server.getHashRange() + ", key is " + key.getString());
            LOG.info("Sending following metadata to client: " + server.getMetadata());
            return new Message(Status.SERVER_NOT_RESPONSIBLE, server.getMetadata());
        }

        switch (message.getStatus()) {
            case GET:
            	if (!server.getMetadata().isReplicaOrCoordinatorKeyrange(key.getString(), server.getHashRange())) {
                    LOG.info("Server not responsible! Server hash range is " + server.getHashRange() + ", key is " + key.getString());
                    return new Message(Status.SERVER_NOT_RESPONSIBLE, server.getMetadata());
                }
                return handleGET(message);
            case PUT:
                if (server.isWriteLocked() && !message.isBatchData()) {
                    LOG.info("Server is write-locked");
                    return new Message(Status.SERVER_WRITE_LOCK);
                }
                return handlePUT(key, val);
            default:
                throw LogUtils.printLogError(LOG, new IllegalArgumentException("Unknown Request Type " + message.getStatus()));
        }
    }

    /**
     * Handles and creates a suitable response for a put request
     *
     * @param key key of the key-value pair
     * @param val value of the key-value pair
     * @return server response
     */
    private IMessage handlePUT(K key, V val) {
        PUTStatus status = cm.put(key, val);
        switch (status) {
            case CREATE_SUCCESS:
                return new Message(Status.PUT_SUCCESS, key, val);
            case CREATE_ERROR:
            case UPDATE_ERROR:
                return new Message(Status.PUT_ERROR, key, val);
            case UPDATE_SUCCESS:
                return new Message(Status.PUT_UPDATE, key, val);
            case DELETE_SUCCESS:
                return new Message(Status.DELETE_SUCCESS, key);
            case DELETE_ERROR:
                return new Message(Status.DELETE_ERROR, key);
            default:
                LOG.error(new IllegalStateException("Unknown PUTStatus " + status));
                throw new IllegalStateException("Unknown PUTStatus " + status);
        }
    }

    /**
     * Handles and creates response for a get request
     *
     * @param message the get-request message sent by a client
     * @return server response to client request
     */
    private IMessage handleGET(IMessage message) {
        V val = cm.get(message.getK());
        return (val == null) ? new Message(Status.GET_ERROR, message.getK())
                : new Message(Status.GET_SUCCESS, message.getK(), val);
    }

    /**
     * Sends out a message
     *
     * @param message Message that is sent
     * @throws IOException
     */
    public void send(IMessage message) throws IOException {
        byte[] toSend = MessageMarshaller.marshall(message);
        output = new BufferedOutputStream(clientSocket.getOutputStream());
        output.write(toSend);
        output.flush();
        LOG.info("SEND " + toSend.length + " bytes \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + "> ===> '"
                + message.toString() + "'");
    }


    /**
     * Receives a message sent by a client
     *
     * @return the received message
     * @throws IOException
     */
    private IMessage receive() throws IOException {
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        input = new BufferedInputStream(clientSocket.getInputStream());
        int justRead = input.read(messageBuffer);

        int inBuffer = justRead;
        while (!MessageMarshaller.isMessageComplete(messageBuffer, inBuffer)) {
            LOG.warn("input hasn't reached EndOfStream, keep reading...");
            justRead = input.read(messageBuffer, inBuffer, messageBuffer.length - inBuffer);
            inBuffer += justRead;
        }

        LOG.info("Read " + inBuffer + " bytes from input stream");

        IMessage message = MessageMarshaller.unmarshall(messageBuffer);

        LOG.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + "> ===>'"
                + message.toString() + "'");
        return message;
    }


    private byte[] copy(int size, byte[] messageBytes, byte[] bufferBytes) {
        byte[] tmp;
        if (messageBytes == null) {
            tmp = new byte[size];
            System.arraycopy(bufferBytes, 0, tmp, 0, size);
        } else {
            tmp = new byte[messageBytes.length + size];
            System.arraycopy(messageBytes, 0, tmp, 0, messageBytes.length);
            System.arraycopy(bufferBytes, 0, tmp, messageBytes.length, size);
        }
        return tmp;
    }


}
