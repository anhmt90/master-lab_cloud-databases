package ecs;

import management.ConfigMessage;
import management.ConfigMessageMarshaller;
import management.ConfigStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.HashUtils;
import util.LogUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;

/**
 * Handles connection from ECS to one key-value storage server
 */
public class KVServer implements Comparable<KVServer> {
    private static final String ECS_LOG = "ECS";
    private static final int SOCKET_TIMEOUT = 6000;
    private static Logger LOG = LogManager.getLogger(ECS_LOG);

    private String hashKey;

    private String nodeName;
    private int servicePort;
    private InetSocketAddress address;
    private Socket socket;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    private String[] sshCMD;

    private final static int RETRY_NUM = 5;
    private final static int RETRY_WAIT_TIME = 1000; // milliseconds

    public KVServer(String serverName, String hostAddress, String servicePort, String adminPort) {
        this(serverName, hostAddress, Integer.parseInt(servicePort), Integer.parseInt(adminPort));
    }

    public KVServer(String serverName, String hostAddress, int servicePort, int adminPort) {
        this(serverName, servicePort, new InetSocketAddress(hostAddress, adminPort));
    }

    public KVServer(String serverName, int servicePort, InetSocketAddress address) {
        this.nodeName = serverName;
        this.servicePort = servicePort;
        this.address = address;

//        String username = servicePort % 2 == 0 ? "anhmt90" : "lab";

        String[] cmds = {"ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
                "tuan-anh@" + getHost(),
                "nohup java -jar /mnt/data/Workspace/uni-project/cloud-databases/gr7-ms3/ms3-server.jar " + nodeName + " " + this.servicePort + " " + getAdminPort()
                        + " > /mnt/data/Workspace/uni-project/cloud-databases/gr7-ms3/logs/" + nodeName + ".log"
                        + " &"
        };
        this.sshCMD = cmds;
        this.hashKey = HashUtils.getHash(String.format("%s:%d", this.getHost(), this.servicePort));
    }

    public String getHost() {
        return this.address.getHostString();
    }

    public int getAdminPort() {
        return this.address.getPort();
    }

    public int getServicePort() {
        return servicePort;
    }

    void launch(Consumer<Boolean> callback) {
        boolean launched = false;
        try {
            execSSH();
            initSocket();
            LOG.info(String.format("Started server %s:%d via ssh. Internal management port at %d", address.getHostString(), getServicePort(), getAdminPort()));
            launched = true;
        } catch (IOException e) {
            LOG.error(String.format("Couldn't launch the server %s:%d with internal management port at %d" + e, this.getHost(), this.getServicePort(), getAdminPort()));
        }
        callback.accept(launched);
    }

    private void execSSH() throws IOException {
        boolean hasError = false;
        byte count = 0;
        while (!hasError) {
            Process proc = Runtime.getRuntime().exec(sshCMD);
            BufferedReader in = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            BufferedReader err = new BufferedReader(new InputStreamReader(proc.getErrorStream()));
            String output;
            while ((output = in.readLine()) != null) {
                LOG.info("SSH output: " + output);
            }
            while ((output = err.readLine()) != null) {
                hasError = true;
                LOG.error("SSH error: " + output);
            }
            count++;
            if (count == RETRY_NUM)
                break;
        }
    }

//    private boolean heartbeat() throws IOException {
//        boolean success = false;
//        for (int i = 0; i < 3; i++) {
//            success = sendAndExpect(new ConfigMessage(ConfigStatus.HEART_BEAT), ConfigStatus.ALIVE);
//            if (success)
//                return success;
//            try {
//                TimeUnit.MILLISECONDS.sleep(RETRY_WAIT_TIME);
//            } catch (InterruptedException e) {
//                LogUtils.printLogError(LOG, e);
//            }
//        }
//        return false;
//
//    }


    /**
     * Sends a message to the connected server
     *
     * @param message message to be sent
     * @throws IOException
     */
    public void send(ConfigMessage message) throws IOException {
        try {
            bos = new BufferedOutputStream(socket.getOutputStream());
            byte[] bytes = ConfigMessageMarshaller.marshall(message);
            bos.write(bytes);
            bos.flush();

            LOG.info("SEND \t<"
                    + socket.getInetAddress().getHostAddress() + ":"
                    + socket.getPort() + ">: '"
                    + message.toString() + "'");

        } catch (IOException e) {
            LOG.error(e);
            throw e;
        }
    }

    /**
     * Receives a message sent by {@link server.app.Server}
     *
     * @return the received message
     * @throws IOException
     */
    private ConfigMessage receive() throws IOException {
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        while (true) {
            try {
                bis = new BufferedInputStream(socket.getInputStream());
                int justRead = bis.read(messageBuffer);
                ConfigMessage message = ConfigMessageMarshaller.unmarshall(Arrays.copyOfRange(messageBuffer, 0, justRead));

                LOG.info("RECEIVE \t<"
                        + socket.getInetAddress().getHostAddress() + ":"
                        + socket.getPort() + ">: '"
                        + message.toString().trim() + "'");
                return message;
            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            }
        }
    }

    boolean init(Metadata metadata, int cacheSize, String strategy) {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.INIT, cacheSize, strategy, metadata);
        return sendAndExpect(msg, ConfigStatus.INIT_SUCCESS);
    }

    boolean startServer() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.START);
        return sendAndExpect(msg, ConfigStatus.START_SUCCESS);
    }

    boolean stopServer() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.STOP);
        return sendAndExpect(msg, ConfigStatus.STOP_SUCCESS);
    }

    boolean shutDown() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.SHUTDOWN);
        return sendAndExpect(msg, ConfigStatus.SHUTDOWN_SUCCESS);
    }

    boolean lockWrite() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.LOCK_WRITE);
        return sendAndExpect(msg, ConfigStatus.LOCK_WRITE_SUCCESS);
    }

    boolean unlockWrite() {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.UNLOCK_WRITE);
        return sendAndExpect(msg, ConfigStatus.UNLOCK_WRITE_SUCCESS);
    }

    boolean moveData(KeyHashRange range, KVServer target) {
        NodeInfo meta = new NodeInfo(target.getNodeName(), target.getHost(), target.getServicePort(), range);
        ConfigMessage msg = new ConfigMessage(ConfigStatus.MOVE_DATA, meta);
        boolean success = false;
        try {
            socket.setSoTimeout(0);
            success = sendAndExpect(msg, ConfigStatus.MOVE_DATA_SUCCESS);
            socket.setSoTimeout(SOCKET_TIMEOUT);
        } catch (SocketException e) {
            LOG.error(e);
        }
        return success;
    }

    boolean update(Metadata metadata) {
        ConfigMessage msg = new ConfigMessage(ConfigStatus.UPDATE_METADATA, metadata);
        return sendAndExpect(msg, ConfigStatus.UPDATE_METADATA_SUCCESS);
    }

    /**
     * Sends a ConfigMessage and checks if the server response matches an expected response
     *
     * @param toSend   message to be sent to the server
     * @param expected status of the expected server response
     * @return true if server response matches the expected one
     * @throws IOException
     */
    private boolean sendAndExpect(ConfigMessage toSend, ConfigStatus expected) {
        try {
            send(toSend);
            ConfigMessage response = receive();
            return response.getStatus().equals(expected);
        } catch (IOException e) {
            LOG.error("Error! ", e);
            return false;
        }
    }

    private void initSocket() {
        LOG.debug("Initializing socket");
        if (socket == null || !socket.isConnected() || socket.isClosed()) {
            LOG.info("Connecting to the server");
            for (int i = 0; i < RETRY_NUM; i++) {
                try {
                    socket = new Socket();
                    socket.setSoTimeout(SOCKET_TIMEOUT);
                    TimeUnit.MILLISECONDS.sleep(RETRY_WAIT_TIME);
                    socket.connect(address, 5000);
                    break;
                } catch (IOException | InterruptedException e) {
                    LOG.error(String.format("Couldn't connect trying again (%d/%d)...", i + 1, RETRY_NUM) + e);
                    if (i == RETRY_NUM - 1)
                        return;
                }
            }
            LOG.info("Connect to server " + address.getHostString() + ":" + address.getPort() + " successfully");
        }
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public String getNodeName() {
        return nodeName;
    }

    public String getHashKey() {
        return hashKey;
    }

    public void closeSocket() throws IOException {
        try {
            socket.close();
            bos.close();
            bis.close();
        } catch (IOException e) {
            LOG.error("Couldn't close socket or streams");
            throw e;
        }
        socket = null;
        bos = null;
        bis = null;
    }

    @Override
    public int compareTo(KVServer kvServer) {
        return this.getHashKey().compareTo(kvServer.getHashKey());
    }
}
