package server.app;

import ecs.KeyHashRange;
import ecs.Metadata;
import ecs.NodeInfo;
import management.IExternalConfigurationService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import server.api.BatchDataTransferProcessor;
import server.api.ClientConnection;
import server.api.InternalConnectionManager;
import server.storage.CacheManager;
import server.storage.cache.CacheDisplacementStrategy;
import util.FileUtils;
import util.HashUtils;

import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;

import static util.FileUtils.WORKING_DIR;

/**
 * Storage server implementation.
 */
public class Server extends Thread implements IExternalConfigurationService {
    public static final String SERVER_LOG = "kvServer";
    private static final String DEFAULT_LOG_LEVEL = "ERROR";
    private static final int DEFAULT_CACHE_SIZE = 100;
    private static final int DEFAULT_PORT = 50000;
    private boolean adminConnected;

    private static Logger LOG = LogManager.getLogger(SERVER_LOG);

    private int servicePort;
    private int adminPort;
    private CacheManager cm;

    NodeState state;
    boolean running;

    private ServerSocket kvSocket;
    /* keeps the range of values that this and other servers are responsible for */
    private Metadata metadata;
    private KeyHashRange hashRange;
    private String serverName;

    private final InternalConnectionManager internalConnectionManager;

    /**
     * Start KV Server at given servicePort
     *
     * @param servicePort given servicePort for disk server to operate
     * @param logLevel    specifies the logging Level on the server
     */
    public Server(String serverName, int servicePort, int adminPort, String logLevel) {
        this.serverName = serverName;
        this.servicePort = servicePort;
        this.adminPort = adminPort;
        Configurator.setRootLevel(Level.getLevel(logLevel));
        state = NodeState.STOPPED;

        internalConnectionManager = new InternalConnectionManager(this);
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

        this.cm = new CacheManager(serverName, cacheSize, getDisplacementStrategyByName(strategy));
        this.metadata = metadata;

        try {
            hashRange = getHashRange(metadata);
        } catch (NoSuchElementException nsee) {
            LOG.error(nsee);
            return false;
        }

        LOG.info("Server initialized with cache size " + cacheSize
                + " and displacement strategy " + strategy);
        return true;
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
                running = false;
            } catch (IOException e) {
                LOG.error("Unable to close internal management socket or KV-socket! \n" + e);
            }
        }
        return !running;
    }

    @Override
    public boolean lockWrite() {
        if (!isStarted())
            return false;
        state = NodeState.WRITE_LOCKED;
        return true;
    }

    @Override
    public boolean unlockWrite() {
        if (!isWriteLocked())
            return false;
        state = NodeState.STARTED;
        return true;
    }

    @Override
    public boolean update(Metadata metadata) {
        try {
            hashRange = getHashRange(metadata);
        } catch (NoSuchElementException nsee) {
            LOG.error(nsee);
            return false;
        }
        this.metadata = metadata;
        return false;
    }


    public boolean moveData(KeyHashRange range, NodeInfo target) {
        if (!range.isSubRangeOf(this.hashRange))
            return false;
        if (!isWriteLocked())
            return false;
        return new BatchDataTransferProcessor(target, cm.getPersistenceManager().getDbPath()).handleTransferData(range);

    }


    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    @Override
    public void run() {
        running = initServer();
        LOG.info("Server's running = " + running);
        new Thread(internalConnectionManager).start();

        if (kvSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = kvSocket.accept();
                    ClientConnection connection = new ClientConnection(this, client, cm);
                    new Thread(connection).start();
                    LOG.info("Client connection initialized");

                    LOG.info(
                            "Connected to " + client.getInetAddress().getHostName() + " on servicePort " + client.getPort());
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
    private boolean initServer() {
        LOG.info("Initialize server ...");
        try {
            kvSocket = new ServerSocket(servicePort);
            LOG.info("Server listening on servicePort: " + kvSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            LOG.error("Error! Cannot poll server socket:");
            if (e instanceof BindException) {
                LOG.error("Port " + servicePort + " is already bound!");
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
     * @return hashRange
     */
    public KeyHashRange getHashRange() {
        return hashRange;
    }

    private KeyHashRange getHashRange(Metadata metadata) throws NoSuchElementException {
        LOG.info(metadata);
        LOG.info("kvSocket.getLocalPort() = " + kvSocket.getLocalPort());
        LOG.info("serverName = " + serverName);
        Optional<NodeInfo> nodeData = metadata.get().stream()
                .filter(md -> md.getPort() == adminPort && md.getName().equals(serverName))
                .findFirst();
        if (!nodeData.isPresent())
            throw new NoSuchElementException("Metadata does not contain info for this node");
        LOG.info("SERVER RANGE = " + nodeData.get().getRange());
        return nodeData.get().getRange();
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

    public String getServerName() {
        return serverName;
    }

    public void setNodeState(NodeState state) {
        this.state = state;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
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
        LOG.info("Server " + server.getServerName() + " created and serving on servicePort " + server.getServicePort());
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
}