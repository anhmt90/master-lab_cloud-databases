package client.api;

import client.mapreduce.Driver;
import client.mapreduce.Job;
import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import management.MessageSerializer;
import mapreduce.common.ApplicationID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.kv.*;
import protocol.kv.IMessage.Status;
import util.HashUtils;
import util.LogUtils;
import util.StringUtils;
import util.Validate;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;

import static protocol.Constants.MAX_BUFFER_LENGTH;

public class Client implements IClient {
    public static final String CLIENT_LOG = "kvClient";
    private static Logger LOG = LogManager.getLogger(Client.CLIENT_LOG);
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
     * List of the storage servers with their addresses
     */
    private Metadata metadata;

    /**
     * Info of the server the client is currently connected to
     */
    private NodeInfo connectedNode;

    private Driver driver;

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
        connectedNode = new NodeInfo(address, port);
    }

    public Client() {

    }

    private KeyHashRange getConnectedRange() {
        return connectedNode.getWriteRange();
    }

    public Metadata getMetadata() {
        return metadata;
    }

    @Override
    public void connect() throws IOException {
        try {
            socket = new Socket();
            socket.connect(new InetSocketAddress(address, port), 5000);
            if(metadata == null)
                requestMetadata();
        } catch (UnknownHostException uhe) {
            throw LogUtils.printLogError(LOG, uhe, "Unknown host");
        } catch (SocketTimeoutException ste) {
            throw LogUtils.printLogError(LOG, ste, "Could not connect to server. Connection timeout.");
        } catch (IOException ioe) {
            throw LogUtils.printLogError(LOG, ioe, "Could not connect to server.");
        }
    }


    @Override
    public void disconnect() {
        try {
            if (socket != null && !socket.isClosed() && socket.isConnected()) {
                socket.shutdownOutput();
                socket.shutdownInput();
            }
            if (bos != null)
                bos.close();
            if (bis != null)
                bis.close();
            if (socket != null) {
                socket.close();
            }
            bos = null;
            bis = null;
            socket = null;
        } catch (IOException e) {
            LogUtils.printLogError(LOG, e, "Connection is already closed.");
        }
    }

    /**
     * sends a KV-Message
     *
     * @param message the report message that needs to be sent
     * @throws IOException
     */
    public void send(byte[] message) throws IOException {
        try {
            bos = new BufferedOutputStream(socket.getOutputStream());
            bos.write(message);
            bos.flush();

            LOG.info("SEND \t<"
                    + socket.getInetAddress().getHostAddress() + ":"
                    + socket.getPort() + ">: '"
                    + message.length + " bytes'");

        } catch (IOException e) {
            LOG.error(e);
            throw e;
        }
    }

    /**
     * Receives a KV-Message
     *
     * @return the received message
     * @throws IOException
     */
    @Override
    public byte[] receive() {
        byte[] messageBuffer = new byte[MAX_BUFFER_LENGTH];
        int justRead = 0;
        while (true) {
            try {
                bis = new BufferedInputStream(socket.getInputStream());
                justRead = bis.read(messageBuffer);

                if (justRead < 0)
                    return null;

                byte[] res = Arrays.copyOfRange(messageBuffer, 0, justRead);

                LOG.info("RECEIVE \t<"
                        + socket.getInetAddress().getHostAddress() + ":"
                        + socket.getPort() + ">: '"
                        + res.length + " bytes'");

                return res;

            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            } catch (IOException e) {
                LOG.error(e);
            }
        }
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
     * Reconnects to the correct server for the key on server miss
     */
    private void reroute() throws IOException {
        if(socket == null && bos == null && bis == null){
            LOG.warn("Client is disconnected");
            return;
        }

        disconnect();
        this.address = connectedNode.getHost();
        this.port = connectedNode.getPort();
        LOG.info("Server isn't responsible for the key. RECONNECTING to server " + address + ":" + port);
        connect();
        Validate.isTrue(isConnected(), "Error when rerouting to new node!");
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
        value = StringUtils.isBlank(value);
        LOG.info("PUT key=" + key + ", value=" + ((value == null) ? "null" : value));
        IMessage message = Message.createPUTMessage(key, value);
        return put(message);
    }

    public IMessage put(IMessage message) throws IOException {
        IMessage serverResponse = null;
        V value = message.getV();
        while (true) {
            selectWriteServer(message.getKeyHashed());
            serverResponse = submit(message);
            if (serverResponse == null)
                return new Message((value == null) ? Status.DELETE_ERROR : Status.PUT_ERROR);

            if (serverResponse.getStatus() == Status.SERVER_NOT_RESPONSIBLE) {
                updateMetadata(serverResponse.getMetadata());
                continue;
            }
            return serverResponse;
        }
    }

    private void selectWriteServer(String keyHashed) throws IOException {
        if (isWithinConnectedRange(keyHashed)) return;

        LOG.info("Selecting an appropriate server to send PUT-request to");
        NodeInfo coordinator = metadata.getCoordinator(keyHashed);
        if (coordinator == null) {
            print("No server found being responsible for the key.");
            throw LogUtils.printLogError(LOG, new IOException(), "No server found responsible for key can't route request.");
        }
        setConnectedNode(coordinator);
        reroute();
    }

    private void selectReadServer(String keyHashed) throws IOException {
        if (isWithinConnectedRange(keyHashed)) return;

        LOG.info("Selecting an appropriate server to send GET-request to");
        NodeInfo nodeForGET = metadata.getNodeToReadFrom(keyHashed);
        setConnectedNode(nodeForGET);
        reroute();
    }

    private boolean isWithinConnectedRange(String keyHashed) {
        KeyHashRange connectedRange = getConnectedRange();
        if (connectedRange != null && connectedRange.contains(keyHashed))
            return true;
        return false;
    }

    private void setConnectedNode(NodeInfo connectedNode) {
        this.connectedNode = connectedNode;
    }

    /**
     * Handles retrying an operation if it targeted the wrong server
     *
     * @param metadata the new metadata
     * @throws IOException
     */
    private void updateMetadata(Metadata metadata)
            throws IOException {
        if (metadata == null)
            throw LogUtils.printLogError(LOG, new IOException(), "Metadata received from server is empty");

        this.metadata = metadata;
    }

    @Override
    public IMessage get(String key) throws IOException {
        String keyHashed = HashUtils.hash(key);
        while (true) {
            selectReadServer(keyHashed);
            IMessage toSend = new Message(Status.GET, new K(key));
            IMessage serverResponse = submit(toSend);
            if (serverResponse == null)
                return new Message(Status.GET_ERROR);
            else if (serverResponse.getStatus() == Status.SERVER_NOT_RESPONSIBLE) {
                updateMetadata(serverResponse.getMetadata());
                continue;
            }
            return serverResponse;
        }
    }

    /**
     * Handles delivering of Messages without a value. For GET and DELETE operations
     *
     * @param message the message to send
     * @return the server response
     * @throws IOException
     */
    private IMessage submit(IMessage message) throws IOException {
        send(MessageSerializer.serialize(message));
        IMessage response = MessageSerializer.deserialize(receive());
        if (response == null)
            LOG.info("Received from server: null");
        else
            LOG.info("Received from server: " + response.toString());
        return response;
    }


    public void handleMRJob(ApplicationID appId, TreeSet<String> input) {
        Validate.isTrue(metadata != null, "Metadata is null");
        driver = new Driver(this);
        driver.exec(new Job(appId, input));
    }

    public void requestMetadata() throws IOException {
        IMessage toSend = new Message(Status.GET_METADATA);
        send(MessageSerializer.serialize(toSend));
        IMessage resp = MessageSerializer.deserialize(receive());
        updateMetadata(resp.getMetadata());
    }




}
