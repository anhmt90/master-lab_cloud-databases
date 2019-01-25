package server.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.*;
import protocol.kv.IMessage.Status;
import server.app.Server;
import server.storage.PUTStatus;
import server.storage.cache.CacheManager;
import util.LogUtils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static protocol.Constants.MAX_KV_MESSAGE_LENGTH;


/**
 * Represents a connection end point for a particular client that is
 * connected to the server. This class is responsible for message reception
 * and sending.
 * The class also implements the echo functionality. Thus whenever a message
 * is received it is going to be echoed back to the client.
 */
public class ClientConnection implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private static final int MAX_ALLOWED_EOF = 3;
    private static final Set<Status> SUCCESS_STATUS = new HashSet<>(Arrays.asList(new Status[]{Status.PUT_SUCCESS, Status.PUT_UPDATE, Status.DELETE_SUCCESS}));
    private boolean isOpen;

    private final Server server;
    private Socket clientSocket;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

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
        IMessage request = null;
        IMessage response;
        int eofCounter = 0;
        try {
            while (isOpen && server.isRunning()) {
                try {
                    request = receive();
                    if (request == null) {
                        eofCounter++;
                        if (eofCounter >= MAX_ALLOWED_EOF) {
                            LOG.warn("Got " + eofCounter + " successive EOF signals! Assume the other end has terminated but not closed the socket properly. " +
                                    "Tearing down connection");
                            isOpen = false;
                        }
                        continue;
                    }
                    response = handleRequest(request);
                    send(response);
                    eofCounter = 0;

                    if (SUCCESS_STATUS.contains(response.getStatus()))
                        replicate(response, request.isBatchData());
                } catch (IOException ioe) {
                    LOG.error("Error! Connection lost!", ioe);
                    LOG.warn("Setting isOpen to false");
                    isOpen = false;
                } catch (IllegalArgumentException iae) {
                    LOG.error("IllegalArgumentException", iae);
                    LOG.error(request.toString());
                    try {
                        send(new Message(Status.PUT_ERROR));
                    } catch (IOException ioe) {
                        LOG.error("Error! Connection lost!", ioe);
                    }
                    LOG.warn(iae);
                } catch (Exception e) {
                    LOG.error("Exception", e);
                    e.printStackTrace();
                    isOpen = false;
                }
            }
        } finally {
            try {
                LOG.warn("CLOSING SOCKET...");
                disconnect();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    public void disconnect() throws IOException {
        LOG.warn("Closing clientSocket=" + clientSocket);
        if (clientSocket != null) {
            clientSocket.shutdownInput();
            clientSocket.shutdownOutput();

            bos.close();
            bis.close();
            clientSocket.close();

            bis = null;
            bos = null;
            clientSocket = null;
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

        switch (message.getStatus()) {
            case GET:
                if (!server.getReadRange().contains(key.getString())) {
                    LOG.info("Server not responsible! Server hash range is " + server.getWriteRange() + ", key is " + key.getString());
                    return new Message(Status.SERVER_NOT_RESPONSIBLE, server.getMetadata());
                }
                return handleGET(message);
            case PUT:
                if (server.isWriteLocked() && !message.isBatchData()) {
                    LOG.info("Server is write-locked");
                    return new Message(Status.SERVER_WRITE_LOCK);
                }
                if(server.getReadRange().contains(key.getString()) && message.isBatchData()) {
                    LOG.info("Message is replicated on the server");
                    return handlePUT(key, val);
                }
                if (!server.getWriteRange().contains(key.getString())) {
                    LOG.info("Server not responsible! Server hash range is " + server.getWriteRange() + ", key is " + key.getString());
                    LOG.info("Sending following metadata to client: " + server.getMetadata());
                    return new Message(Status.SERVER_NOT_RESPONSIBLE, server.getMetadata());
                }
                return handlePUT(key, val);
            case GET_METADATA:
                    LOG.info("Sending following metadata to client: " + server.getMetadata());
                    return new Message(Status.METADATA, server.getMetadata());

            default:
                throw LogUtils.printLogError(LOG, new IllegalArgumentException("Unknown Request Type " + message.getStatus()));
        }
    }

    private void replicate(IMessage message, boolean isRequestBatchData) {
        if (!isRequestBatchData) {
            message.setBatchData();
            server.getReplicator1().setMessage(message);
            new Thread(server.getReplicator1()).start();

            server.getReplicator2().setMessage(message);
            new Thread(server.getReplicator2()).start();
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
        bos = new BufferedOutputStream(clientSocket.getOutputStream());
        bos.write(toSend);
        bos.flush();
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
        byte[] messageBuffer = new byte[MAX_KV_MESSAGE_LENGTH];
        bis = new BufferedInputStream(clientSocket.getInputStream());
        int justRead = bis.read(messageBuffer);

        int inBuffer = justRead;
        while (justRead > 0 && !MessageMarshaller.isMessageComplete(messageBuffer, inBuffer)) {
            LOG.info("bis hasn't reached EndOfStream, keep reading...");
            justRead = bis.read(messageBuffer, inBuffer, messageBuffer.length - inBuffer);
            if (justRead > 0)
                inBuffer += justRead;
        }

        if (inBuffer == -1) {
            LOG.warn("RECEIVE -1 bytes (EOS), returning null.");
            return null;
        }
        LOG.info("received " + inBuffer + " bytes from server");
        IMessage message = MessageMarshaller.unmarshall(messageBuffer);

        LOG.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + "> ===>'"
                + message.toString() + "'");
        return message;
    }
}
