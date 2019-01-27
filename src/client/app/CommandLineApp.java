package client.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import client.api.Client;
import mapreduce.common.ApplicationID;
import protocol.kv.IMessage;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import util.StringUtils;

/**
 *
 */
public class CommandLineApp {

    private static final String WHITESPACE = StringUtils.WHITE_SPACE;
    static Logger LOG = LogManager.getLogger(Client.CLIENT_LOG);

    private static final int MAX_VALUE_SIZE = (120 * 1024) - 1;
    private static final int MAX_KEY_LENGTH = 20;
    private static final String CONNECT = "connect";
    private static final String DISCONNECT = "disconnect";
    private static final String PUT = "put";
    private static final String LOG_LEVEL = "logLevel";
    private static final String HELP = "help";
    private static final String QUIT = "quit";
    private static final String GET = "get";

    private static final String COUNT = "count";



    /**
     * The StorageClient as an instance of {@link Client} to communicate with the
     * StorageServer
     */
    private static Client kvClient = new Client();

    /**
     * Gets user inputs and calls the respective methods to handle the input
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        Scanner input = new Scanner(System.in);

        while (true) {
            printCommandPrompt();
            String userInput = input.nextLine();
            if(StringUtils.isEmpty(userInput))
                continue;
            LOG.info("User input: " + userInput);
            String[] cmdComponents = userInput.split(StringUtils.WHITE_SPACE, 2);
            String commandName = cmdComponents[0];

            switch (commandName) {
                case CONNECT:
                    handleConnect(cmdComponents);
                    break;
                case DISCONNECT:
                    handleDisconnect();
                    break;
                case PUT:
                    handlePut(cmdComponents);
                    break;
                case GET:
                    handleGet(cmdComponents);
                    break;

                case COUNT:
                    handleCount(cmdComponents);
                    break;

                case LOG_LEVEL:
                    handleLogLevel(cmdComponents);
                    break;
                case HELP:
                    printHelp();
                    break;
                case QUIT:
                    handleQuit(input);
                    return;
                default:
                    print("Unknown command\n");
                    printHelp();
            }
        }
    }

    private static void handleCount(String[] cmdComponents) throws IOException {
        if (!isClientConnected())
            return;

        if (!isValidArgs(COUNT, cmdComponents)) {
            return;
        }
        String keyword = cmdComponents[1];

        if (!kvClient.isClosed()) {
            LOG.info("Counting occurences of " + keyword);

            kvClient.handleMRJob(ApplicationID.WORD_COUNT, new HashSet<>(Arrays.asList(new String[]{keyword})));
//            handleServerResponse(serverResponse, key);
        } else {
            print("No connection currently established. Please establish a connection before retrieving a value");
        }
    }

    /**
     * Handles the command {@see LOG_LEVEL}
     *
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second
     */
    private static void handleLogLevel(String[] cmdComponents) {
        if (!isValidArgs(LOG_LEVEL, cmdComponents)) {
            return;
        }
        String lvl = cmdComponents[1];
        Level level = Level.getLevel(lvl);
        if (level != null) {
            Configurator.setRootLevel(level);
            LOG.info("Logging level is set to: " + level.toString());
        } else {
            String msg = "Logging level \"" + lvl + "\" is unknown";
            System.out.println(msg);
            LOG.info(msg);
        }
    }

    /**
     * Handles the command {@see QUIT}
     *
     * @param input The scanner for input stream from System.in
     */
    private static void handleQuit(Scanner input) {
        print("Exiting application");
        LOG.info("quit");
        kvClient.disconnect();
        input.close();
    }

    /**
     * Handles the command {@see PUT}
     *
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second
     */
    private static void handlePut(String[] cmdComponents) throws IOException {
        if (!isClientConnected())
            return;

        if (!isValidArgs(PUT, cmdComponents))
            return;

        String[] keyAndValue = cmdComponents[1].split(StringUtils.WHITE_SPACE, 2);
        String key = keyAndValue[0];
        if (key.length() < 1) {
            print("Key needs to be at least one non whitespace character.");
            LOG.info("Invalid key. " + cmdComponents);
            return;
        }
        if (key.length() > MAX_KEY_LENGTH) {
            String msg = "key exceeds maximum length";
            print(msg);
            LOG.info(msg + ": " + key.length());
            return;
        }
        if (keyAndValue.length == 1) {
            handleDelete(key);
            return;
        }

        String value = keyAndValue[1];
        if (value.length() > MAX_VALUE_SIZE) {
            String msg = "Value exceeds maximum loadedDataSize";
            print(msg);
            LOG.info(msg + ": " + value.length());
            return;
        }

        if (!kvClient.isClosed()) {
            LOG.info("Putting: ", key, value);

            IMessage serverResponse = kvClient.put(key, value);
            handleServerResponse(serverResponse, key);
        } else {
            print("No connection currently established. Please establish a connection before storing a key-value pair.");
        }
    }

    /**
     * Handles deletion of values as a functionality of the put() command
     *
     * @param key Key for which the corresponding value will be deleted
     */
    private static void handleDelete(String key) throws IOException {
        if (!kvClient.isClosed()) {
            LOG.info("Deleting: ", key);

            IMessage serverResponse = kvClient.put(key, null);
            handleServerResponse(serverResponse, key);
        } else {
            print("No connection currently established. Please establish a connection before deleting a key-value pair.");
        }
    }

    /**
     * Handles the command {@see GET}
     *
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second
     */
    private static void handleGet(String[] cmdComponents) throws IOException {
        if (!isClientConnected())
            return;

        if (!isValidArgs(GET, cmdComponents)) {
            return;
        }
        String key = cmdComponents[1];
        if (key.length() > MAX_KEY_LENGTH) {
            String msg = "Key exceeds maximum length";
            print(msg);
            LOG.info(msg + ": " + key.length());
            return;
        }
        if (!kvClient.isClosed()) {
            LOG.info("Getting: ", key);

            IMessage serverResponse = kvClient.get(key);
            handleServerResponse(serverResponse, key);
        } else {
            print("No connection currently established. Please establish a connection before retrieving a value");
        }
    }

    /**
     * Prints out the appropriate response to a server response and logs info
     * 
     * @param serverResponse responser from server to a request
     * @param key key belonging to the request
     */
    private static void handleServerResponse(IMessage serverResponse, String key) {
    	switch (serverResponse.getStatus()) {
	    	case PUT_ERROR:
	            print("Fail to store key-value pair.");
	            LOG.info("Storage failure");
	            break;
	        case PUT_SUCCESS:
	            print("Key-value pair stored successfully.");
	            LOG.info("Storage success");
	            break;
	        case PUT_UPDATE:
	        	print("Value for " + key + "was updated.");
	        	LOG.info("Update success");
	        	break;
	        case GET_ERROR:
	            print("Retrieving value was unsuccesful. There might be no value saved on the server corresponding to the given key: "
	                    + key);
	            LOG.info("Get failure");
	            break;
	        case GET_SUCCESS:
	            print("Value stored on server for key '" + key + "' is: " + serverResponse.getValue());
	            LOG.info("Get success");
	            break;
	        case DELETE_ERROR:
                print("Fail to remove entry. No value for key " + key + " found on server.");
                LOG.info("Deletion failure");
                break;
            case DELETE_SUCCESS:
                print("Value stored on server for key " + key + " was deleted.");
                LOG.info("Deletion success");
                break;
            case SERVER_STOPPED:
            	print("Storage server is currently stopped and does not accept client requests.");
            	LOG.info("Server stopped");
            	break;
            case SERVER_WRITE_LOCK:
            	print("Storage server does not currently accept put requests.");
            	LOG.info("Server locked");
            	break;
	        default:
	            print("Unknown server response. Please try again");
	            LOG.info("Incompatible server response", serverResponse);
	    }
    }
    
    /**
     * Disconnects the connection to StorageServer
     */
    private static void handleDisconnect() {
        if (!isClientConnected())
            return;
        kvClient.disconnect();
        String msg = "Storage client disconnected.";
        print(msg);
        LOG.info(msg);

    }

    /**
     * Handles the command {@see CONNECT}
     *
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second
     */
    private static void handleConnect(String[] cmdComponents) throws IOException {
        if (!isValidArgs(CONNECT, cmdComponents)) {
            return;
        }

        if (kvClient != null && kvClient.isConnected() && !kvClient.isClosed()) {
            String msg = "Client is already connected to a server. Please disconnect before establishing another connection.";
            print(msg);
            LOG.info(msg);
            return;
        }
        String commandArgs = cmdComponents[1];
        String[] connectionInfo = commandArgs.split(StringUtils.WHITE_SPACE);
        if (!isValidArgs(CONNECT, connectionInfo) || !isValidPortNumber(connectionInfo[1]))
            return;

        String address = connectionInfo[0];
        int port = Integer.parseInt(connectionInfo[1]);

        kvClient = new Client(address, port);
        kvClient.connect();
    }

    /**
     * Receives and format text message from StorageServer
     *
     * @return The message received from StorageServer in string format
     */
    private static String receiveTextFromServer() {
        byte[] bytes = kvClient.receive();
        String message = new String(bytes, StandardCharsets.US_ASCII).trim();
        LOG.info("Bytes from server: ", bytes);
        LOG.info("Message from server: " + message);
        return message;
    }

    /**
     * Prints help text giving reference for each command and purpose of the
     * application
     */
    public static void printHelp() {
        print("This application works as an storage client. The command set is as follows:\n" + getUsage(CONNECT)
                + getUsage(DISCONNECT) + getUsage(PUT) + getUsage(GET) + getUsage(LOG_LEVEL) + getUsage(HELP)
                + getUsage(QUIT)

        );
    }

    /**
     * Get the usage for a specified command
     *
     * @param commandName The name of the command to get the usage for
     * @return the usage of the command provided in the parameter
     */
    private static String getUsage(String commandName) {
        switch (commandName) {
            case CONNECT:
                return "'connect <address> <port>' - establish a TCP connection to specified server\n";
            case DISCONNECT:
                return "'disconnect' - disconnect from currently connected server\n";
            case PUT:
                return "'put <key> <value>' - store a key-value pair on the server. Leaving the value field empty will delete the value stored on the server corresponding to the given key\n";
            case GET:
                return "'get <key>' - retrieve value for the given key from the storage server\n";
            case LOG_LEVEL:
                return "'logLevel <level>' - set logger to specified level\n";
            case HELP:
                return "'help' - display list of commands\n";
            case QUIT:
                return "'quit' - end any ongoing connections and stop the application\n";
            default:
                return ("Unknown command");
        }
    }

    /**
     * @param commandName The name of the command to print the usage for
     */
    private static void printUsage(String commandName) {
        System.out.print("\nUsage: " + getUsage(commandName));
    }

    /**
     * Prints the command prompt 'StorageClient>' to System.out
     */
    private static void printCommandPrompt() {
        System.out.print("\nStorageClient> ");
    }

    /**
     * Prints an output string to System.out
     *
     * @param output The output string to print to System.out
     */
    private static void print(String output) {
        System.out.print(output);
    }

    /**
     * @return boolean value indicating the StorageClient is still connected with the
     * StorageServer or not
     */
    private static boolean isClientConnected() {
        if (kvClient.isClosed() || !kvClient.isConnected()) {
            String msg = "Client is not currently connected to server.";
            print(msg);
            LOG.info(msg);
            return false;
        }
        return true;
    }

    /**
     * Checks whether the {@param portAsString} is a valid port number
     *
     * @param portAsString The port number in string format
     * @return boolean value indicating the {@param portAsString} is a valid port
     * number or not
     */
    private static boolean isValidPortNumber(String portAsString) {
        if (portAsString == null || portAsString.equals("")) {
            print("Port number not provided");
            return false;
        }
        if (!portAsString
                .matches("^([0-9]{1,4}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$")) {
            print("Invalid port number. Port number should contain only digits and range from 0 to 65535.");
            return false;
        }
        return true;
    }

    /**
     * Checks whether the command arguments of a specified {@param commandName} are
     * valid
     *
     * @param commandName   The command name
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second.
     * @return boolean value indicating the arguments associating with the
     * {@see commandName} are valid or not
     */
    private static boolean isValidArgs(String commandName, String[] cmdComponents) {
        switch (commandName) {
            case CONNECT:
            case LOG_LEVEL:
                if (cmdComponents.length != 2)
                    return handleInvalidArgs(commandName, cmdComponents);
                break;
            case PUT:
                if (cmdComponents.length != 2 || cmdComponents[1].split(WHITESPACE).length > 2)
                    return handleInvalidArgs(commandName, cmdComponents);
                break;
            case GET:
                if (cmdComponents.length != 2 || cmdComponents[1].split(WHITESPACE).length > 1)
                    return handleInvalidArgs(commandName, cmdComponents);
                break;
        }
        return true;
    }

    /**
     * prints to console and log if user provided illegal arguments
     * 
     * @param commandName    name of the command for which the user
     *                      provided wrong arguments
     * @param cmdComponents String array containing the user arguments
     * @return false
     */
    private static boolean handleInvalidArgs(String commandName, String[] cmdComponents) {
        print("Invalid argument for '" + commandName + "' command");
        LOG.info("Invalid argument. " + cmdComponents);
        printUsage(commandName);
        return false;
    }
}
