package client.api;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import protocol.*;
import protocol.IMessage.*;

public class Client implements IClient {

    private static Logger LOG = LogManager.getLogger(Client.class);
    /**
     * The client socket
     */
    private Socket socket;

    /**
     * The output stream for sending data to server
     */
    private BufferedOutputStream bos;

    /**
     * the input stream for receiving data to server
     */
    private BufferedInputStream bis;

    private String address;
    private int port;


    /**
     * Creates a new client and opens a client socket to immediately connect to the
     * server identified by the parameters
     *
     * @param address The address of the server to connect to
     * @param port    The port number that server is listening to
     */
    public Client(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public Client() {

    }

    @Override
    public void connect() throws IOException {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(address, port), 5000);
        } catch (UnknownHostException uhe) {
            throw printLogError(uhe, "Unknown host");
        } catch (SocketTimeoutException ste) {
            throw printLogError(ste, "Could not connect to server. Connection timeout.");
        } catch (IOException ioe) {
            throw printLogError(ioe, "Could not connect to server.");
        }
    }

    private <E> E printLogError(E exception, String message) {
        print(message + "\n");
        LOG.error(message, exception);
        return exception;
    }

    @Override
    public void disconnect() {
        try {
            if (bos != null)
                bos.close();
            if (bis != null)
                bis.close();
            if (socket != null) {
                socket.close();
            }
            socket = new Socket();
        } catch (IOException e) {
            printLogError(e, "Connection is already closed.");
        }
    }

    @Override
    public void send(byte[] data) throws IOException {
        try {
            bos = new BufferedOutputStream(socket.getOutputStream());
            bos.write(data);
            bos.flush();
            LOG.info("sending " + data.length + " bytes to server");
        } catch (IOException e) {
            disconnect();
            throw printLogError(e, "Could't connect to the server. Disconnecting...");
        }
    }

    @Override
    public byte[] receive() {
        byte[] data = new byte[2 + 20 + 1024 * 120];
        try {
            socket.setSoTimeout(50000);
            bis = new BufferedInputStream(socket.getInputStream());
            int bytesCopied = bis.read(data);
            LOG.info("received data from server" + bytesCopied + " bytes");
        } catch (SocketTimeoutException ste) {
            printLogError(ste, "'receive' timeout. Client will disconnect from server.");
            disconnect();
        } catch (IOException e) {
            printLogError(e, "Could't connect to the server. Disconnecting...");
            disconnect();
        }
        return data;
    }

    @Override
    public boolean isClosed() {
        LOG.debug(socket);
        if (socket != null)
            LOG.debug(socket.isClosed());
        return socket == null || socket.isClosed();
    }

    @Override
    public boolean isConnected() {
        if (socket != null)
            return socket.isConnected();
        else
            return false;
    }

    /**
     * Prints an output string to System.out
     *
     * @param output The output string to print to System.out
     */
    private static void print(String output) {
        System.out.print(output);
    }

    @Override
    public IMessage put(String key, String value) throws IOException {
        if (value != null && value.equals("null"))
            value = null;
        if (value != null) {
            return storeOnServer(key, value);
        } else {
            return removeOnServer(key);
        }
    }

    private IMessage removeOnServer(String key) throws IOException {
        return sendWithoutValue(key, Status.PUT);
    }

    @Override
    public IMessage get(String key) throws IOException {
        return sendWithoutValue(key, Status.GET);
    }

    private IMessage sendWithoutValue(String key, Status status) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.US_ASCII);
        IMessage toSend = new Message(status, new K(keyBytes));
        send(MessageMarshaller.marshall(toSend));
        IMessage response = MessageMarshaller.unmarshall(receive());
        LOG.debug(response);
        return response;
    }

    private IMessage storeOnServer(String key, String value) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.US_ASCII);
        byte[] valueBytes = value.getBytes(StandardCharsets.US_ASCII);
        IMessage toSend = new Message(Status.PUT, new K(keyBytes), new V(valueBytes));
        send(MessageMarshaller.marshall(toSend));
        IMessage response = MessageMarshaller.unmarshall(receive());
        LOG.info("Received from server: " + response);
        return response;
    }
}
