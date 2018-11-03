package server.api;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import protocol.IMessage;
import protocol.Message;
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

    private static Logger logger = LogManager.getLogger(ClientConnection.class);

    private boolean isOpen;
    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 128 * BUFFER_SIZE;

    private Socket clientSocket;
    private ObjectInputStream input;
    private ObjectOutputStream output;

    /**
     * Constructs a new CientConnection object for a given TCP socket.
     *
     * @param clientSocket the Socket object for the client connection.
     */
    public ClientConnection(Socket clientSocket) {
        this.clientSocket = clientSocket;
        this.isOpen = true;
    }

    /**
     * Initializes and starts the client connection.
     * Loops until the connection is closed or aborted by the client.
     */
    public void run() {
        try {
            output = new ObjectOutputStream(clientSocket.getOutputStream());
            input = new ObjectInputStream(clientSocket.getInputStream());

            sendText("Connection to KV Storage Server established: "
                            + clientSocket.getLocalAddress() + " / "
                            + clientSocket.getLocalPort());

            while (isOpen) {
                try {
                    IMessage kvMessage = receive();

                    /* connection either terminated by the client or lost due to
                     * network problems*/
                } catch (IOException ioe) {
                    logger.error("Error! Connection lost!");
                    isOpen = false;
                }
            }

        } catch (IOException ioe) {
            logger.error("Error! Connection could not be established!", ioe);

        } finally {

            try {
                if (clientSocket != null) {
                    input.close();
                    output.close();
                    clientSocket.close();
                }
            } catch (IOException ioe) {
                logger.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    /**
     * Method sends a TextMessage using this socket.
     *
     * @param msg the message that is to be sent.
     * @throws IOException some I/O error regarding the output stream
     */
    public void sendText(String msg) throws IOException {
        byte[] messageBytes = StringUtils.toByteArray(msg);
        output.write(messageBytes, 0, messageBytes.length);
        output.flush();
        logger.info("SEND \t<"
                + clientSocket.getInetAddress().getHostAddress() + ":"
                + clientSocket.getPort() + ">: '"
                + msg + "'");
    }


    private IMessage receive() throws IOException {

        int index = 0;
        byte[] messageBytes = null;
        byte[] bufferBytes = new byte[BUFFER_SIZE];

        /* read first char from stream */
        byte read = (byte) input.read();
        boolean reading = true;

        while (read != 13 && reading) {/* carriage return */
            /* if buffer filled, copy to msg array */
            if (index == BUFFER_SIZE) {
                messageBytes = copy(BUFFER_SIZE, messageBytes, bufferBytes);
                bufferBytes = new byte[BUFFER_SIZE];
                index = 0;
            }

            /* only read valid characters, i.e. letters and constants */
            bufferBytes[index] = read;
            index++;

            /* stop reading since DROP_SIZE is reached */
            if (messageBytes != null && messageBytes.length + index >= DROP_SIZE) {
                reading = false;
            }

            /* read next char from stream */
            read = (byte) input.read();
        }

        /* handle cases where we reach carriage return and reading is still TRUE */
        messageBytes = copy(index, messageBytes, bufferBytes);

        /* build final String */
        IMessage message = Message.build(messageBytes);

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
