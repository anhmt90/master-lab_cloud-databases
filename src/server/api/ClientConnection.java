package server.api;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import protocol.*;
import protocol.IMessage.Status;
import server.app.Server;
import server.storage.PUTStatus;
import server.storage.CacheManager;
import util.HashUtils;
import util.StringUtils;

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

    private static final int MAX_MESSAGE_LENGTH = 2 + 20 + 1024 * 120;
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
        try {
            output = new BufferedOutputStream(clientSocket.getOutputStream());
            input = new BufferedInputStream(clientSocket.getInputStream());

            send("Connection to KV Storage Server established: "
                    + clientSocket.getLocalAddress() + " / "
                    + clientSocket.getLocalPort());

            while (isOpen) {
                try {
                    IMessage requestMessage = receive();
                    IMessage responseMessage = handleRequest(requestMessage);
                    send(responseMessage);

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
        if (server.isStopped())
            return new Message(Status.SERVER_STOPPED);

        K key = message.getK();
        V val = message.getV();

        if (!server.getHashRange().inRange(key.getString())) {
            return new Message(Status.SERVER_NOT_RESPONSIBLE, server.getMetadata());
        }

        switch (message.getStatus()) {
            case GET:
                return handleGET(message);
            case PUT:
                if (server.isWriteLocked())
                    return new Message(Status.SERVER_WRITE_LOCK);
                return handlePUT(key, val);
            default:
                throw new IllegalArgumentException("Unknown Request Type");
        }
    }

    /**
     * Handles and creates a suitable response for a put request
     *
     * @param key key of the key-value pair
     * @param val value of the key-value pair
     * @return server response
     */
    private synchronized IMessage handlePUT(K key, V val) {
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
                throw new IllegalStateException("Unknown PUTStatus");
        }
    }

    /**
     * Handles and creates response for a get request
     *
     * @param message the get-request message sent by a client
     * @return server response to client request
     */
    private synchronized IMessage handleGET(IMessage message) {
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
        writeOutput(message.toString(), MessageMarshaller.marshall(message));
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
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + object + "'");
    }


    /**
     * Receives a message sent by a client
     *
     * @return the received message
     * @throws IOException
     */
    private IMessage receive() throws IOException {
        int index = 0;
        byte[] messageBytes = new byte[MAX_MESSAGE_LENGTH];

        int bytesCopied = input.read(messageBytes);
        LOG.info("Read " + bytesCopied + " from input stream");

        IMessage message = MessageMarshaller.unmarshall(messageBytes);

        LOG.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + message.toString().trim() + "'");
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
