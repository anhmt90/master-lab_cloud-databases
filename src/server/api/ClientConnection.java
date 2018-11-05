package server.api;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import protocol.*;
import protocol.IMessage.Status;
import server.storage.PUTStatus;
import server.storage.CacheManager;
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

    private static Logger LOG = LogManager.getLogger(ClientConnection.class);

    private boolean isOpen;

    private Socket clientSocket;
    private BufferedInputStream input;
    private BufferedOutputStream output;

    private CacheManager cm;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket, CacheManager cm) {
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
                    IMessage kvMessage = receive();
                    send(handleRequest(kvMessage));

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
        K key = message.getKey();
        V val = message.getValue();
        switch (message.getStatus()) {
            case GET:
                return handleGET(message);
            case PUT:
                return handlePUT(key, val);
            default:
                throw new IllegalArgumentException("Unknown Request Type");
        }
    }

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

    private synchronized IMessage handleGET(IMessage message) {
        V val = cm.get(message.getKey());
        return (val == null) ? new Message(Status.GET_ERROR, message.getKey())
                : new Message(Status.GET_SUCCESS, message.getKey(), val);
    }

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

    private void writeOutput(String object, byte[] messageBytes) throws IOException {
        output.write(messageBytes);
        output.flush();
        LOG.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + object + "'");
    }


    private IMessage receive() throws IOException {
        int index = 0;
        byte[] messageBytes = new byte[2 + 20 + 1024 * 120];
        ;

        /* read first char from stream */
        int bytesCopied = input.read(messageBytes);


        IMessage message = MessageMarshaller.unmarshall(messageBytes);
        /* TODO
        logger.info("RECEIVE \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + message.getMessage().trim() + "'");*/
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
