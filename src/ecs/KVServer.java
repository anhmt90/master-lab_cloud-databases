package ecs;

import management.ConfigMessage;
import management.ConfigMessageMarshaller;
import management.ConfigStatus;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;
import static util.FileUtils.SEP;
import static util.FileUtils.WORKING_DIR;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.HashUtils;

public class KVServer implements Comparable<KVServer> {
    private static final String ECS_LOG = "ECS";
    private static Logger LOG = LogManager.getLogger(ECS_LOG);

    public String getHashKey() {
        return hashKey;
    }

    private String hashKey;

    private InetSocketAddress address;
    private Socket socket;
    private int sshPort = 22;
    private int mgmtPort;
    private String sshCMD;

    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    public KVServer(String host, int port, int mgmtPort) throws IOException {
        this.address = new InetSocketAddress(host, port);
        this.mgmtPort = mgmtPort;

        initSocketIfNotConnected();
        this.sshCMD = String.format("ssh -n %s -p %d nohup java -jar " + WORKING_DIR + SEP + "ms3-server.jar %d %d &",
                host,
                sshPort,
                port,
                mgmtPort);
        this.hashKey = HashUtils.getHash(String.format("%s:%d", host, port));
    }

    public void send(ConfigMessage message) throws IOException {
        initSocketIfNotConnected();
        try {
            bos.write(ConfigMessageMarshaller.marshall(message));
            bos.flush();
            LOG.info("SEND \t<"
                    + socket.getInetAddress().getHostAddress() + ":"
                    + socket.getPort() + ">: '"
                    + message.toString() + "'");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Receives a message sent by {@link server.app.Server}
     *
     * @return the received message
     * @throws IOException
     */
    private ConfigMessage receive() throws IOException {
        initSocketIfNotConnected();
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        int bytesCopied = bis.read(messageBuffer);
        LOG.info("Read " + bytesCopied + " from input stream");

        ConfigMessage message = ConfigMessageMarshaller.unmarshall(messageBuffer);

        LOG.info("RECEIVE \t<"
                + socket.getInetAddress().getHostAddress() + ":"
                + socket.getPort() + ">: '"
                + message.toString().trim() + "'");

        return message;
    }

    public String getHost() {
        return this.address.getHostString();
    }

    public int getPort() {
        return this.address.getPort();
    }

    public String getHostPort(){
        return getHost() + ":" + getPort();
    }

    void launch() throws IOException, InterruptedException {
        Process proc;
        Runtime run = Runtime.getRuntime();
        proc = run.exec(this.sshCMD);
        proc.waitFor();
        LOG.info(String.format("Started server %s:%d via ssh", this.address.getHostString(), this.address.getPort()));
    }

    boolean init(Metadata metadata, int cacheSize, String strategy) {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.INIT, cacheSize, strategy, metadata);
        try {
            return sendAndExpect(msg, ConfigStatus.INIT_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean startServer() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.START);
        try {
            return sendAndExpect(msg, ConfigStatus.START_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean stopServer() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.STOP);
        try {
            return sendAndExpect(msg, ConfigStatus.STOP_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean shutDown() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.SHUTDOWN);
        try {
            return sendAndExpect(msg, ConfigStatus.SHUTDOWN_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean lockWrite() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.LOCK_WRITE);
        try {
            return sendAndExpect(msg, ConfigStatus.LOCK_WRITE_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean unlockWrite() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.UNLOCK_WRITE);
        try {
            return sendAndExpect(msg, ConfigStatus.UNLOCK_WRITE_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean moveData(KeyHashRange range, KVServer anotherServer) {
        NodeInfo meta = new NodeInfo(anotherServer.getHost(), anotherServer.getPort(), range);
        ConfigMessage msg = new ConfigMessage(ConfigStatus.MOVE_DATA, meta);
        try {
            return sendAndExpect(msg, ConfigStatus.MOVE_DATA_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    boolean update(Metadata metadata) {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.UPDATE_METADATA, metadata);
        try {
            return sendAndExpect(msg, ConfigStatus.UPDATE_METADATA_SUCCESS);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean sendAndExpect(ConfigMessage toSend, ConfigStatus expected) throws IOException {
        send(toSend);
        ConfigMessage response = receive();
        if (response.getStatus().equals(expected))
            return true;
        return false;
    }

    private void initSocketIfNotConnected() {
        if (socket == null || !socket.isConnected() || socket.isClosed()) {
            socket = new Socket();
            try {
                socket.connect(new InetSocketAddress(getHost(), getPort()), 5000);
                socket.setSoTimeout(5000);
                bos = new BufferedOutputStream(socket.getOutputStream());
                bis = new BufferedInputStream(socket.getInputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public Socket getSocket() {
        return socket;
    }

    public void setSocket(Socket socket) {
        this.socket = socket;
    }

    @Override
    public int compareTo(KVServer kvServer) {
        return this.getHashKey().compareTo(kvServer.getHashKey());
    }
}
