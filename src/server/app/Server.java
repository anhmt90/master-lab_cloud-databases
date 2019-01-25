package server.app;

import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import management.IExternalConfigurationService;
import mapreduce.server.TaskReceiver;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import server.api.*;
import server.storage.cache.CacheDisplacementStrategy;
import server.storage.cache.CacheManager;
import util.FileUtils;
import util.Validate;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import static server.Constants.HEARTBEAT_RECEIVE_PORT_DISTANCE;
import static util.FileUtils.WORKING_DIR;

/**
 * Storage server implementation.
 */
public class Server extends Thread implements IExternalConfigurationService {
    public static final String SERVER_LOG = "kvServer";
    private static Logger LOG = LogManager.getLogger(SERVER_LOG);

    private static final String DEFAULT_LOG_LEVEL = "ERROR";
    private Replicator replicator1;
    private Replicator replicator2;


    /**
     * Expected time interval to receive a heartbeat from predecessor
     */
    public static final int HEARTBEAT_INTERVAL = 3000;

    private int servicePort;
    private int adminPort;
    private CacheManager cm;

    NodeState state;
    NodeState previousState;
    private boolean running;

    private ServerSocket kvSocket;
    /* keeps the range of values that this and other servers are responsible for */
    private Metadata metadata;
    private KeyHashRange writeRange;
    private KeyHashRange readRange;
    private String serverId;

    private final InternalConnectionManager internalConnectionManager;


    private HeartbeatReceiver heartbeatReceiver;
    private HeartbeatSender heartbeatSender;

    private TaskReceiver taskReceiver;


    /**
     * Start KV Server at given servicePort
     *
     * @param servicePort given servicePort for disk server to operate
     * @param logLevel    specifies the logging Level on the server
     */
    public Server(String serverId, int servicePort, int adminPort, String logLevel) {
        this.serverId = serverId;
        this.servicePort = servicePort;
        this.adminPort = adminPort;
        Configurator.setRootLevel(Level.getLevel(logLevel));
        state = NodeState.STOPPED;

        internalConnectionManager = new InternalConnectionManager(this);
        taskReceiver = new TaskReceiver(this);
        LOG.info("Server constructed with servicePort " + this.servicePort + " and  with logging Level " + logLevel);

    }

    /**
     * @param metadata  info about assignment of key ranges on servers in the ring
     * @param cacheSize specifies how many key-value pairs the server is allowed to
     *                  keep in-memory
     * @param strategy  specifies the storage replacement strategy in case the
     *                  storage is full and there is a GET- or PUT-request on a key
     *                  that is currently not contained in the storage. Options are
     *                  "FIFO", "LRU", and "LFU".
     * @return
     */
    @Override
    public boolean initKVServer(Metadata metadata, int cacheSize, String strategy) {
        if (!isValidCacheSize(cacheSize)) {
            LOG.error("Invalid cache size");
            return false;
        }

        if (!isValidDisplacementStrategy(strategy)) {
            LOG.error("Invalid displacement strategy");
            return false;
        }

        this.cm = new CacheManager(serverId, cacheSize, getDisplacementStrategyByName(strategy));
        boolean success = update(metadata);
        if (success)
            LOG.info("Server initialized with cache size " + cacheSize
                    + " and displacement strategy " + strategy);
        return success;
    }

    private void startHeartbeat(Metadata metadata) {
        if(metadata.getLength() == 1)
            return;
        LOG.info("Starting heartbeat receiver...");
        this.heartbeatReceiver = new HeartbeatReceiver(this);
        new Thread(heartbeatReceiver).start();

        LOG.info("Starting heartbeat sender...");
        NodeInfo successor = metadata.getSuccessor(writeRange);
        this.heartbeatSender = new HeartbeatSender(successor.getHost(), successor.getPort() + HEARTBEAT_RECEIVE_PORT_DISTANCE);
        new Thread(heartbeatSender).start();
    }

    /**
     * Stops the server insofar that it won't listen at the given servicePort any more.
     */
    @Override
    public boolean stopService() {
        if (isStarted() || isWriteLocked()) {
            state = NodeState.STOPPED;
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                LOG.error(e);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean startService() {
        if (!isStopped())
            return false;
        state = NodeState.STARTED;
        return true;
    }

    @Override
    public boolean shutdown() {
        if (state.equals(NodeState.STOPPED) || stopService()) {
            try {
                internalConnectionManager.getAdminSocket().close();
                kvSocket.close();
                heartbeatReceiver.close();
                heartbeatSender.close();
                taskReceiver.close();
                running = false;
            } catch (IOException e) {
                LOG.error("Unable to close internal management socket or KV-socket! \n" + e);
            }
        }
        return !running;
    }

    @Override
    public boolean lockWrite() {
        previousState = state;
        state = NodeState.WRITE_LOCKED;
        return true;
    }

    @Override
    public boolean unlockWrite() {
        if (!isWriteLocked())
            return false;
        state = previousState;
        previousState = null;
        return true;
    }

    @Override
    public boolean update(Metadata metadata) {
        Metadata oldMetadata = this.metadata;
        KeyHashRange oldWriteRange = writeRange;
        KeyHashRange oldReadRange = readRange;
        KeyHashRange oldWriteRangeOfReplica2 = null;
        if (replicator2 != null)
            oldWriteRangeOfReplica2 = replicator2.getReplica().getWriteRange();

        this.metadata = metadata;
        try {
            updateWriteRange();
            updateReadRange();
        } catch (NoSuchElementException nsee) {
            LOG.error(nsee);
            return false;
        }
        setReplicas();

        LOG.info("Current Metadata = " + this.metadata);

        if (heartbeatSender != null && heartbeatReceiver != null) {
            heartbeatReceiver.close();
            heartbeatSender.close();
        }
        startHeartbeat(metadata);

        DataReconciliationHandler reconciler = new DataReconciliationHandler(this)
                .withOldMetadata(oldMetadata)
                .withOldReadRange(oldReadRange)
                .withOldWriteRange(oldWriteRange)
                .withOldWriteRangeOfReplica2(oldWriteRangeOfReplica2);

        return reconciler.reconcile();
    }

    private void setReplicas() {
        NodeInfo replica1 = metadata.getSuccessor(writeRange);
        replicator1 = new Replicator(replica1);

        NodeInfo replica2 = metadata.getSuccessor(replica1.getWriteRange());
        replicator2 = new Replicator(replica2);
    }


    /**
     * Moves data from this server within a specified range to a target server
     * 
     * @param range hashrange that should be transported
     * @param target the server the data is moved to
     * @return true if move successful
     */
    public boolean moveData(KeyHashRange range, NodeInfo target) {
        LOG.info("handle Move data with range " + range + " and with target " + target);
        if (!isWriteLocked()) {
            LOG.error("Not in state " + NodeState.WRITE_LOCKED + ". Current state is " + state);
            return false;
        }
        LOG.info("Moving data to " + target.getId());
        return new BatchDataTransferProcessor(target, cm.getPersistenceManager().getDbPath()).handleTransferData(range);

    }



    private ConcurrentHashMap<String, ClientConnection> clientConnectionTable;

    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    @Override
    public void run() {
        running = openServiceSocket();
        LOG.info("Server's running = " + running);

        Validate.notNull(internalConnectionManager, "internalConnectionManager is null");
        Validate.notNull(taskReceiver, "taskReceiver is null");

        new Thread(internalConnectionManager).start();
        new Thread(taskReceiver).start();

        if (kvSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = kvSocket.accept();
                    ClientConnection connection = new ClientConnection(this, client, cm);
                    new Thread(connection).start();
                    LOG.info("Client connection initialized");

                    LOG.info(
                            "Connected to " + client.getInetAddress().getHostName() + " on servicePort " + kvSocket.getLocalPort());
                } catch (IOException e) {
                    LOG.error("Error! " + "Unable to establish connection. \n", e);
                }
            }
        }
        LOG.info("Server stopped.");
    }

    /**
     * Initializes the server socket
     *
     * @return boolean value indicating if socket was successfully set up
     */
    private boolean openServiceSocket() {
        try {
            kvSocket = new ServerSocket(servicePort);
            LOG.info("Server listening on servicePort: " + kvSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            LOG.error("Error! Cannot poll server socket:");
            if (e instanceof BindException) {
                LOG.error("Port " + servicePort + " is already bound!", e);
            }
            return false;
        }
    }


    /**
     * Gets metadata
     *
     * @return metadata
     */
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Gets range of hash-values that the server is responsible for storing
     *
     * @return writeRange
     */
    public KeyHashRange getWriteRange() {
        return writeRange;
    }

    public KeyHashRange getReadRange() {
        return readRange;
    }

    private void updateWriteRange() throws NoSuchElementException {
        LOG.info(metadata);
        LOG.info("kvSocket.getLocalPort() = " + kvSocket.getLocalPort());
        LOG.info("serverId = " + serverId);

        int i = metadata.getIndexById(serverId);
        writeRange = metadata.get(i).getWriteRange();
    }


    public void updateReadRange() {
        int i = metadata.getIndexById(serverId);
        int index2ndPredecessor = (i - 2 + metadata.getLength()) % metadata.getLength();
        KeyHashRange secondPredecessorRange = metadata.get(index2ndPredecessor).getWriteRange();
        readRange = new KeyHashRange(secondPredecessorRange.getStart(), writeRange.getEnd());
    }

    /**
     * Gets cache manager
     *
     * @return Cache Manager
     */
    public CacheManager getCacheManager() {
        return cm;
    }


    /**
     * Gets current service state of the server
     *
     * @return the one of the state listed in {@link NodeState}
     */
    public NodeState getNodeState() {
        return state;
    }

    public int getServicePort() {
        return servicePort;
    }

    public String getServerId() {
        return serverId;
    }

    public void setNodeState(NodeState state) {
        this.state = state;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    /**
     * Checks if server is running
     *
     * @return true if server is running
     */
    public boolean isRunning() {
        return running;
    }

    public boolean isStarted() {
        return state.equals(NodeState.STARTED);
    }

    public boolean isStopped() {
        return state.equals(NodeState.STOPPED);
    }

    public boolean isWriteLocked() {
        return state.equals(NodeState.WRITE_LOCKED);
    }

    /**
     * Checks whether the {@param portAsString} is a valid servicePort number
     *
     * @param portAsString The servicePort number in string format
     * @return boolean value indicating the {@param portAsString} is a valid servicePort
     * number or not
     */
    private static boolean isValidPortNumber(String portAsString) {
        if (portAsString.matches("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$"))
            return true;
        LOG.error("Invalid servicePort number. Port number should contain only digits and range from 0 to 65535.");
        return false;
    }

    /**
     * Checks whether the {@param cacheSizeString} is a valid cache loadedDataSize
     *
     * @param cacheSize The cache loadedDataSize to be checked
     * @return boolean value indicating the {@param cacheSize} is a valid cache
     * loadedDataSize or not
     */
    private static boolean isValidCacheSize(int cacheSize) {
        if (cacheSize >= 1 && cacheSize <= 1073741824)
            return true;
        else {
            LOG.error("Invalid cache size. Cache Size has to be a number between 1 and 2^30. Provided cacheSize is " + cacheSize);
            return false;
        }
    }

    private boolean isValidDisplacementStrategy(String strategy) {
        String[] validStrategies = {"FIFO", "LRU", "LFU"};
        return Arrays.stream(validStrategies).anyMatch(strategy::equals);
    }

    /**
     * Returns a corresponding Cache Displacement Strategy for a String
     *
     * @param strategy Displacement Strategy in String format
     * @return CacheDisplacementStrategy corresponding to the given String
     * in {@param strategy}
     * @throws IllegalArgumentException if the {@param strategy} is not a
     *                                  recognized Displacement Strategy
     */
    private static CacheDisplacementStrategy getDisplacementStrategyByName(String strategy)
            throws IllegalArgumentException {
        switch (strategy.toUpperCase()) {
            case "FIFO":
                return CacheDisplacementStrategy.FIFO;
            case "LRU":
                return CacheDisplacementStrategy.LRU;
            case "LFU":
                return CacheDisplacementStrategy.LFU;
            default:
                throw new IllegalArgumentException("Invalid displacement strategy.");
        }
    }

    /**
     * Checks whether the {@param logLevel} is a valid logging level
     *
     * @param logLevel The logging level in String format
     * @return boolean value indicating the {@param logLevel} is a valid logging
     * level or not
     */
    private static boolean isValidLogLevel(String logLevel) {
        String[] logLevels = {"ALL", DEFAULT_LOG_LEVEL, "DEBUG", "WARN", "ERROR", "FATAL", "OFF"};
        for (int i = 0; i < logLevels.length; i++) {
            if (logLevel.contentEquals(logLevels[i])) {
                return true;
            }
        }
        System.out.println(
                "Invalid Log Level. Please choose one of the following: 'ALL', 'INFO', 'DEBUG', 'WARN', 'ERROR', 'FATAL', 'OFF'");
        return false;
    }


    /**
     * Main entry point for the echo server application.
     *
     * @param args contains the servicePort number at args[0], the cache loadedDataSize at args[1], the cache displacement strategy at args[2] and the logging Level at args[3].
     */
    public static void main(String[] args) throws IOException {
        Path logDir = Paths.get(WORKING_DIR + "/logs");
        System.out.println("LOGS DIR ==========================================================> " + logDir);
        if (!FileUtils.dirExists(logDir))
            Files.createDirectories(logDir);

        Server server = createServer(args);
        LOG.info("Server instance " + server.getServerId() + " created and is about to serve on servicePort " + server.getServicePort());
        server.start();
    }


    private static Server createServer(String[] args) {
        if (args.length < 3 || args.length > 4)
            throw new IllegalArgumentException("Server name and servicePort must be provided to start the server");

        String serverName = args[0];
        String portString = args[1];
        String adminPortString = args[2];
        String logLevel = DEFAULT_LOG_LEVEL;
        if (args.length == 4 && isValidLogLevel(args[3]))
            logLevel = args[3];

        int port = isValidPortNumber(portString) ? Integer.parseInt(portString) : -1;
        int adminPort = isValidPortNumber(adminPortString) ? Integer.parseInt(adminPortString) : -1;

        if (port < 0 || adminPort < 0) {
            IllegalArgumentException e = new IllegalArgumentException("Invalid service servicePort or administration servicePort!");
            LOG.error(e);
            throw e;
        }

        return new Server(serverName, port, adminPort, logLevel);
    }

    public int getAdminPort() {
        return adminPort;
    }


    public Replicator getReplicator1() {
        return replicator1;
    }

    public Replicator getReplicator2() {
        return replicator2;
    }
}