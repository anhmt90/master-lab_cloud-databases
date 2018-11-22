package server.app;

import ecs.Metadata;
import ecs.NodeInfo;
import management.IExternalConfigurationService;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import server.api.ECSConnection;
import ecs.KeyHashRange;
import server.api.ClientConnection;
import server.storage.cache.CacheDisplacementStrategy;
import server.storage.CacheManager;
import util.LogUtils;
import util.StringUtils;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static util.FileUtils.SEP;
import static util.StringUtils.EMPTY_STRING;

/**
 * Storage server implementation.
 */
public class Server extends Thread implements IExternalConfigurationService {
    public static final String SERVER_LOG = "kvServer";
    private static final String DEFAULT_LOG_LEVEL = "INFO";
    private static final int DEFAULT_CACHE_SIZE = 100;
    private static final int DEFAULT_PORT = 50000;

    private static final String DEFAULT_ECS_ADDRESS = "127.0.0.1";

    private static final int MGMT_PORT = 9867;

    private static Logger LOG = LogManager.getLogger("SERVER_LOG");

    private int port;
    private CacheManager cm;

    NodeState state;
    boolean running;

    private ServerSocket kvSocket;
    /* keeps the range of values that this and other servers are responsible for */
    private Metadata metadata;
    private KeyHashRange hashRange;


    /**
     * Start KV Server at given port
     *
     * @param port     given port for disk server to operate
     * @param logLevel specifies the logging Level on the server
     */
    public Server(int port, String logLevel) {
        this.port = port;
        Configurator.setRootLevel(Level.getLevel(logLevel));
        state = NodeState.STOPPED;

        LOG.info("Server created listening on port " + this.port + " with logging Level " + logLevel);

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

        this.cm = new CacheManager(cacheSize, getDisplacementStrategyByName(strategy));
        this.metadata = metadata;

        try {
            hashRange = getHashRange(metadata);
        } catch (NoSuchElementException nsee) {
            return LogUtils.exitWithError(LOG, nsee);
        }

        LOG.info("Server initialized with cache size " + cacheSize
                + " and displacement strategy " + strategy);
        return true;
    }

    private KeyHashRange getHashRange(Metadata metadata) throws NoSuchElementException {
        Optional<NodeInfo> nodeData = metadata.get().stream()
                .filter(md -> md.getPort() == kvSocket.getLocalPort() && md.getHost().equals(kvSocket.getInetAddress().getHostAddress()))
                .findFirst();
        if (!nodeData.isPresent())
            throw new NoSuchElementException("Metadata does not contain info for this node");
        return nodeData.get().getRange();
    }


    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    @Override
    public boolean stopService() {
        if (isStarted() || isWriteLocked()) {
            try {
                kvSocket.close();
                state = NodeState.STOPPED;
                return true;
            } catch (IOException e) {
                LOG.error("Error! " + "Unable to close socket on port: " + port, e);
            }
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
        if (stopService())
            running = false;
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
            return LogUtils.exitWithError(LOG, nsee);
        }
        this.metadata = metadata;
        return false;
    }


    public boolean moveData(KeyHashRange range, NodeInfo target) {
        if (!range.isSubRangeOf(this.hashRange))
            return false;
        if (!isWriteLocked())
            return false;
        String start = range.getStart();
        String end = range.getEnd();
        String commonPrefix = StringUtils.getLongestCommonPrefix(start, end);
        String commonParentFolder = commonPrefix.replaceAll(".", EMPTY_STRING + SEP);




        for (int i = commonPrefix.length(); i < start.length(); i++) {
            byte currStartDigit = Byte.parseByte(String.valueOf(start.charAt(i)), 16);
            byte currEndDigit = Byte.parseByte(String.valueOf(end.charAt(i)), 16);

            Integer.toHexString(currStartDigit + 1);
//            List<String> fileNames = getFilesInMiddleFullRange(commonPrefix , )
        }

        try {
            Files.walk(Paths.get(commonParentFolder))
                    .filter(Files::isRegularFile)
                    .forEach(System.out::println);
        } catch (IOException ioe) {
            return LogUtils.exitWithError(LOG, ioe);
        }

        return false;
    }


    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    public void run() {
        running = initServer();
        if (kvSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = kvSocket.accept();
                    ClientConnection connection = new ClientConnection(this, client, cm);
                    new Thread(connection).start();

                    LOG.info(
                            "Connected to " + client.getInetAddress().getHostName() + " on port " + client.getPort());
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
            kvSocket = new ServerSocket(port);
            LOG.info("Server listening on port: " + kvSocket.getLocalPort());
            return true;
        } catch (IOException e) {
            LOG.error("Error! Cannot open server socket:");
            if (e instanceof BindException) {
                LOG.error("Port " + port + " is already bound!");
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

    public void setNodeState(NodeState state) {
        this.state = state;
    }

    /**
     * Checks if server is running
     *
     * @return true if server is running
     */
    private boolean isRunning() {
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
     * Checks whether the {@param portAsString} is a valid port number
     *
     * @param portAsString The port number in string format
     * @return boolean value indicating the {@param portAsString} is a valid port
     * number or not
     */
    private static boolean isValidPortNumber(String portAsString) {
        if (portAsString.matches("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$"))
            return true;
        LOG.error("Invalid port number. Port number should contain only digits and range from 0 to 65535.");
        return false;
    }

    /**
     * Checks whether the {@param cacheSizeString} is a valid cache size
     *
     * @param cacheSize The cache size to be checked
     * @return boolean value indicating the {@param cacheSize} is a valid cache
     * size or not
     */
    private static boolean isValidCacheSize(int cacheSize) {
        if (cacheSize > 1 && cacheSize < 1073741824)
            return true;
        else {
            LOG.error("Invalid cache size. Cache Size has to be a number between 1 and 2^30.");
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
     * @param args contains the port number at args[0], the cache size at args[1], the cache displacement strategy at args[2] and the logging Level at args[3].
     */
    public static void main(String[] args) {
        String ecsAddress = DEFAULT_ECS_ADDRESS;
        Server server = createServer(args, ecsAddress);
        server.start();

        ECSConnection ecsConnection = new ECSConnection(ecsAddress, server);

        if (ecsConnection.isOpen() && server.isStopped()) {
            while (server.isRunning()) {
                ecsConnection.pollRequests();
            }
        }

    }


    private static Server createServer(String[] args, String ecsAddress) {
        if (args.length == 0 || args.length > 3)
            throw new IllegalArgumentException("Port number must be provided to start the server");
        int port = -1;
        if (isValidPortNumber(args[0]))
            port = Integer.parseInt(args[0]);

        switch (args.length) {
            case 1:
                return new Server(port, DEFAULT_LOG_LEVEL);
            case 2:
                if (isValidLogLevel(args[1]))
                    return new Server(port, args[1]);
            case 3:
                if (isValidAddress(args[2])) {
                    ecsAddress = args[2];
                    return new Server(port, args[1]);
                }
                throw new IllegalArgumentException("Invalid ECS IP address");
            default:
                throw new IllegalArgumentException("Error! Invalid number of arguments! Number of args provided: " + args.length);
        }
    }

    private static boolean isValidAddress(String address) {
        if (address.matches("^((0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)\\.){3}(0|1\\d?\\d?|2[0-4]?\\d?|25[0-5]?|[3-9]\\d?)$"))
            return true;
        LOG.error("Invalid IP address. IP address should contain 4 octets separated by '.' (dot). Each octet comprises only digits and ranges from 0 to 255.");
        return false;
    }
}