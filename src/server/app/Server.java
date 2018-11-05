package server.app;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import server.api.ClientConnection;
import server.storage.cache.CacheDisplacementStrategy;
import server.storage.CacheManager;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Represents a simple Echo Server implementation.
 */
public class Server extends Thread {
    public static final String SERVER_LOG = "kvServer";
    private static final String DEFAULT_LOG_LEVEL = "INFO";

	private static Logger LOG = LogManager.getLogger("SERVER_LOG");

    private int port;
    private ServerSocket serverSocket;
    private boolean running;
    private CacheManager cm;

    /**
     * Start KV Server at given port
     *
     * @param port      given port for disk server to operate
     * @param cacheSize specifies how many key-value pairs the server is allowed to
     *                  keep in-memory
     * @param strategy  specifies the storage replacement strategy in case the
     *                  storage is full and there is a GET- or PUT-request on a key
     *                  that is currently not contained in the storage. Options are
     *                  "FIFO", "LRU", and "LFU".
     * @param logLevel  specifies the logging Level on the server
     */
    public Server(int port, int cacheSize, CacheDisplacementStrategy strategy, String logLevel) {
        this.port = port;
        this.cm = new CacheManager(cacheSize, strategy);
        Level level = Level.getLevel(logLevel);
        Configurator.setRootLevel(level);

        String toPrint = "Server started at port " + this.port + ", with cache size " + cacheSize
                + ", with cache strategy " + strategy.name() + " and with logging Level " + logLevel;
        LOG.info(toPrint);
        System.out.println(toPrint);

    }

    /**
     * Initializes and starts the server. Loops until the the server should be
     * closed.
     */
    public void run() {
        running = initializeServer();
        if (serverSocket != null) {
            while (isRunning()) {
                try {
                    Socket client = serverSocket.accept();
                    ClientConnection connection = new ClientConnection(client, cm);
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

    private boolean isRunning() {
        return this.running;
    }

    /**
     * Stops the server insofar that it won't listen at the given port any more.
     */
    public void stopServer() {
        running = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            LOG.error("Error! " + "Unable to close socket on port: " + port, e);
        }
    }

    private boolean initializeServer() {
        LOG.info("Initialize server ...");
        try {
            serverSocket = new ServerSocket(port);
            LOG.info("Server listening on port: " + serverSocket.getLocalPort());
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
     * Checks whether the {@param portAsString} is a valid port number
     *
     * @param portAsString The port number in string format
     * @return boolean value indicating the {@param portAsString} is a valid port
     *         number or not
     */
    private static boolean isValidPortNumber(String portAsString) {
        if (portAsString == null || portAsString.equals("")) {
            System.out.println("Port number not provided");
            return false;
        }
        if (!portAsString
                .matches("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$")) {
            System.out
                    .println("Invalid port number. Port number should contain only digits and range from 0 to 65535.");
            return false;
        }
        return true;
    }

    /**
     * Checks whether the {@param cacheSizeString} is a valid cache size
     *
     * @param cacheSizeString The cache size number in string format
     * @return boolean value indicating the {@param cacheSizeString} is a valid cache
     *         size or not
     */
    private static boolean isValidCacheSize(String cacheSizeString) {
        try {
            int cacheSizeInt = Integer.parseInt(cacheSizeString);
            if (cacheSizeInt > 1 && cacheSizeInt < 1073741824) {
                return true;
            }
        } catch (NumberFormatException nex) {

        }
        System.out.println("Invalid cache size. Cache Size has to be a number between 1 and 1073741824.");
        return false;
    }

    /**
     * Returns a corresponding Cache Displacement Strategy for a String
     *
     * @param strategy Displacement Strategy in String format
     * @return CacheDisplacementStrategy corresponding to the given String
     *         in {@param strategy}
     * @throws IllegalArgumentException if the {@param strategy} is not a
     * 		   recognized Displacement Strategy
     */
    private static CacheDisplacementStrategy isValidDisplacementStrategy(String strategy)
            throws IllegalArgumentException {
        switch (strategy) {
            case "FIFO":
                return CacheDisplacementStrategy.FIFO;
            case "LRU":
                return CacheDisplacementStrategy.LRU;
            case "LFU":
                return CacheDisplacementStrategy.LFU;
            default:
                throw new IllegalArgumentException();
        }
    }

    /**
     * Checks whether the {@param logLevel} is a valid logging level
     *
     * @param logLevel The logging level in String format
     * @return boolean value indicating the {@param logLevel} is a valid logging
     *         level or not
     */
    private static boolean isValidLogLevel(String logLevel) {
        String[] logLevels = { "ALL", DEFAULT_LOG_LEVEL, "DEBUG", "WARN", "ERROR", "FATAL", "OFF" };
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
    	switch (args.length) {
            case 0:
                new Server(50000, 100, CacheDisplacementStrategy.FIFO, DEFAULT_LOG_LEVEL).start();
                break;
            case 1:
                if (isValidPortNumber(args[0])) {
                    new Server(Integer.parseInt(args[0]), 100, CacheDisplacementStrategy.FIFO, DEFAULT_LOG_LEVEL).start();
                }
                break;
            case 2:
                if (isValidPortNumber(args[0]) && isValidCacheSize(args[1])) {
                    new Server(Integer.parseInt(args[0]), Integer.parseInt(args[1]), CacheDisplacementStrategy.FIFO,
                            DEFAULT_LOG_LEVEL).start();
                }
                break;
            case 3:
                if (isValidPortNumber(args[0]) && isValidCacheSize(args[1])) {
                    try {
                        new Server(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
                                isValidDisplacementStrategy(args[2]), DEFAULT_LOG_LEVEL).start();;
                    } catch (IllegalArgumentException iaex) {
                        System.out.println(
                                "Invalid Displacement Strategy. Please choose one of the following: 'FIFO', 'LRU', 'LFU'");
                    }
                }
                break;
            case 4:
                if (isValidPortNumber(args[0]) && isValidCacheSize(args[1]) && isValidLogLevel(args[3])) {
                    try {
                        new Server(Integer.parseInt(args[0]), Integer.parseInt(args[1]),
                                isValidDisplacementStrategy(args[2]), args[3]).start();;
                    } catch (IllegalArgumentException iaex) {
                        System.out.println(
                                "Invalid Displacement Strategy. Please choose one of the following: 'FIFO', 'LRU', 'LFU'");
                    }
                }
                break;
            default:
                System.out.println("Error! Invalid number of arguments!");
                System.out.println("Usage: Server <port> <cache_size> <displacement_strategy> <log_level>!");
                System.out.println(
                        "Any arguments at that you do not provide will be filled with the default values {50000, 100, FIFO, INFO}.");
                System.out.println(
                        "Please recognize that the structure of 'Usage' has to be kept intact, e.g. you can not declare a <cache_size> argument without providing a <port> argument beforehand.");
                System.out.println(
                        "<port>: \t port number that the server should listen to. An integer in range [1024, 65535]");
                System.out.println(
                        "<cach_size>: \t maximum number of <K,V> pairs can be cached at the same time. An integer in range [1, 2^30]");
                System.out.println(
                        "<displacement_strategy>: \t specifies the order in which <K,V> pairs should be removed from cache when the cache has reache its <cache_size>. "
                                + "\n\t\t This can be one of the following values {'FIFO', 'LRU', 'LFU'}");
                System.out.println(
                        "<log_level>: \t specifies logging level on server. This can be one of the following values {'ALL', 'INFO', 'DEBUG', 'WARN', 'ERROR', 'FATAL', 'OFF'}");
                return;
        }

    }


    public CacheManager getCacheManager() {
        return cm;
    }
}
