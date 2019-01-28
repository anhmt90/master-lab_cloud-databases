package client.app;

import static util.FileUtils.SEP;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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

    private static final String MAPFILE_PATH = System.getProperty("user.dir") + SEP + "mapreduceResults" + SEP;
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

    private static final String COUNT = "wordcount";
    private static final String SEARCH = "search";

    private static TreeMap<String, String> resultsMR;

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
            if (StringUtils.isEmpty(userInput))
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
                case SEARCH:
                    handleSearch(cmdComponents);
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
        if (!kvClient.isClosed()) {
            LOG.info("Counting occurences of all words");
            resultsMR = kvClient.handleMRJob(ApplicationID.WORD_COUNT, new TreeSet<>());
            printResultsMR();
        } else {
            print("No connection currently established. Please establish a connection before retrieving a value");
        }
    }

    private static void handleSearch(String[] cmdComponents) throws IOException {
        if (!isClientConnected())
            return;

        if (!isValidArgs(SEARCH, cmdComponents)) {
            return;
        }
        if (!kvClient.isClosed()) {
            String searchTerm = cmdComponents[1].contains(StringUtils.WHITE_SPACE) ? stripQuotionMark(cmdComponents[1]) : cmdComponents[1];
            List<String> searchTerms = extractSearchTerms(searchTerm);

            LOG.info("Invert Indexing " + searchTerm);
            resultsMR = kvClient.handleMRJob(ApplicationID.INVERTED_INDEX, new TreeSet<>(searchTerms));
            printResultsMR();
        } else {
            print("No connection currently established. Please establish a connection before retrieving a value");
        }
    }

    private static List<String> extractSearchTerms(String searchTerm) {
        String[] words = searchTerm.split("\\s+");
        System.out.println(Arrays.toString(words));

        List<String> res = new ArrayList<>();
        for (int i = 0; i < words.length; i++) {
            for (int j = i + 1; j <= words.length ; j++) {
                String[] substrings = Arrays.copyOfRange(words, i, j);
                StringBuilder sb = new StringBuilder();
                for (String word : substrings) {
                    sb.append(word + " ");
                }
                sb.setLength(sb.length() - 1);
                res.add(sb.toString());
            }
        }
        return res;
    }


    private static void printResultsMR() {
        String fiveTabs = "                    ";
        System.out.println(fiveTabs + "TO LOOKUP" + fiveTabs + "RESULTS" + fiveTabs);
        System.out.println("_______________________________________________________________________________________");
        for (Entry entry : resultsMR.entrySet()) {
            String val = entry.getValue().toString();
            val = val.replaceAll("\n", "; ");

            System.out.print(fiveTabs);
            System.out.print(entry.getKey());
            System.out.print(fiveTabs);
            System.out.print(val);
            System.out.print(fiveTabs);
            System.out.println();
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
            print("Key needs to be at least one non-whitespace character.");
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

        String value = stripQuotionMark(keyAndValue[1]);
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

    private static String stripQuotionMark(String s) {
        return s.trim().substring(1, s.length() - 1);
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
     * @param key            key belonging to the request
     */
    private static void handleServerResponse(IMessage serverResponse, String key) {
        switch (serverResponse.getStatus()) {
            case PUT_ERROR:
                println("Fail to store key-value pair.");
                LOG.info("Storage failure");
                break;
            case PUT_SUCCESS:
                println("Key-value pair stored successfully.");
                LOG.info("Storage success");
                break;
            case PUT_UPDATE:
                println("Value for '" + key + "' was updated.");
                LOG.info("Update success");
                break;
            case GET_ERROR:
                println("Retrieving value was unsuccesful. There might be no value saved on the server corresponding to the given key: "
                        + key);
                LOG.info("Get failure");
                break;
            case GET_SUCCESS:
                println("Value stored on server for key '" + key + "' is: " + serverResponse.getValue());
                LOG.info("Get success");
                break;
            case DELETE_ERROR:
                println("Fail to remove entry. No value for key " + key + " found on server.");
                LOG.info("Deletion failure");
                break;
            case DELETE_SUCCESS:
                println("Value stored on server for key " + key + " was deleted.");
                LOG.info("Deletion success");
                break;
            case SERVER_STOPPED:
                println("Storage server is currently stopped and does not accept client requests.");
                LOG.info("Server stopped");
                break;
            case SERVER_WRITE_LOCK:
                println("Storage server does not currently accept put requests.");
                LOG.info("Server locked");
                break;
            default:
                println("Unknown server response. Please try again");
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
     * Takes a ConcurrentHashMap containing key value pairs and handles output onto console and a separate file for persistent storage
     *
     * @param map   the hashmap containing the kV pairs
     * @param input a scanner handling input
     */
    private static void handleHashmapPrint(ConcurrentHashMap<String, String> map, Scanner input) {

        if (map == null) {
            print("Map is null.");
            return;
        }

        if (map.isEmpty()) {
            print("Map is empty.");
            return;
        }

        String date = new SimpleDateFormat("dd-MM-yyyy").format(new Date());
        BufferedWriter writer = null;
        try {
            File resultsFile = new File(MAPFILE_PATH + date);
            resultsFile.getParentFile().mkdirs();
            resultsFile.createNewFile();

            writer = new BufferedWriter(new FileWriter(resultsFile));

            for (Entry<String, String> entry : map.entrySet()) {
                writer.newLine();
                writer.newLine();
                String key = entry.getKey();
                String value = entry.getValue();
                writer.write("key: " + key);
                writer.newLine();
                writer.write(value);
            }
            print("HashMap Results were written to file: " + resultsFile.getCanonicalPath());

        } catch (Exception e) {
            LOG.error("Error trying to write Hashmap to file.");
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                LOG.error("Failed trying to close the FileWriter");
            }
        }


        Set<Entry<String, String>> entrySet = map.entrySet();
        Iterator<Entry<String, String>> iterator = entrySet.iterator();

        while (iterator.hasNext()) {
            print("Type <next> to display the next 10 entries in the hashmap. Any other input will return you to the application.");
            String userInput = input.nextLine();
            LOG.info("User input: " + userInput);
            switch (userInput) {
                case "next":
                    for (int i = 0; i < 10; i++) {
                        if (iterator.hasNext()) {
                            Entry<String, String> entry = iterator.next();
                            String key = entry.getKey();
                            String value = entry.getValue();
                            String[] valueSplit = value.split("\n");
                            System.out.printf("%-40s %-60s\n", key, valueSplit[0]);
                            for (int j = 1; j < valueSplit.length; j++) {
                                System.out.printf("%-40s %-60s\n", " ", valueSplit[j]);
                            }
                        } else {
                            print("Reached end of HashMap.");
                            return;
                        }
                    }
                default:
                    return;
            }
        }
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
                + getUsage(DISCONNECT) + getUsage(PUT) + getUsage(GET) + getUsage(COUNT) + getUsage(SEARCH)+ getUsage(LOG_LEVEL) + getUsage(HELP)
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
                return "'put <key> <value>' - store a key-value pair on the server. " +
                        "Leaving the value field empty will delete the value stored on the server corresponding to the given key. " +
                        "Attention: 'key' cannot contain whitespace and 'value' can contain whitespace if is enclosed by quotation marks, e.g. \"this is value\" \n";
            case GET:
                return "'get <key>' - retrieve value for the given key from the storage server\n";
            case COUNT:
                return "'wordcount' - counts occurence number of each word stored in the distributed storage system\n";
            case SEARCH:
                return "'search <search_terms>' - search for files that contain the <search_term>. <search_term> can contain whitespace if is enclosed by quotation marks\n";
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
        System.out.print("\nUSAGE: " + getUsage(commandName));
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
     * Prints an output string to System.out with newline at the end
     *
     * @param output The output string to print to System.out
     */
    private static void println(String output) {
        System.out.println(output);
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
     * Checks whether the command arguments of a specified {@param command} are
     * valid
     *
     * @param command       The command name
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second.
     * @return boolean value indicating the arguments associating with the
     * {@see command} are valid or not
     */
    private static boolean isValidArgs(String command, String[] cmdComponents) {
        String[] args;
        switch (command) {
            case CONNECT:
            case LOG_LEVEL:
                if (cmdComponents.length != 2)
                    return handleInvalidArgs(command, cmdComponents);
                break;
            case PUT:
                if (cmdComponents.length != 2)
                    return handleInvalidArgs(command, cmdComponents);
                args = cmdComponents[1].split(WHITESPACE, 2);
                if (hasWhiteSpace(args[0])) {
                    return handleInvalidArgs(command, cmdComponents);
                }

                if (args[1].contains(StringUtils.WHITE_SPACE) && !isQuoted(args[1])) {
                    print("Value is not quoted properly. ");
                    return handleInvalidArgs(command, cmdComponents);
                }
                break;
            case GET:
                if (cmdComponents.length != 2)
                    return handleInvalidArgs(command, cmdComponents);
                if (hasWhiteSpace(cmdComponents[1]))
                    return handleInvalidArgs(command, cmdComponents);
                break;
            case SEARCH:
                if (cmdComponents.length != 2)
                    return handleInvalidArgs(command, cmdComponents);

                String searchTerm = cmdComponents[1];
                if (searchTerm.contains(StringUtils.WHITE_SPACE) && !isQuoted(searchTerm)) {
                    print("Search term with whitespace is not properly quoted. ");
                    return handleInvalidArgs(command, cmdComponents);
                }
                break;
        }
        return true;
    }

    private static boolean hasWhiteSpace(String key) {
        if (key.contains(StringUtils.WHITE_SPACE)) {
            print("Key cannot contain whitespace. ");
            return true;
        }
        return false;
    }

    private static boolean isQuoted(String quotedString) {
        String s = quotedString.trim();
        return (s.indexOf(StringUtils.QUOTE) == 0 && s.lastIndexOf(StringUtils.QUOTE) == s.length() - 1)
                || (s.indexOf(StringUtils.TICK) == 0 && s.lastIndexOf(StringUtils.TICK) == s.length() - 1);
    }

    /**
     * prints to console and log if user provided illegal arguments
     *
     * @param command       name of the command for which the user
     *                      provided wrong arguments
     * @param cmdComponents String array containing the user arguments
     * @return false
     */
    private static boolean handleInvalidArgs(String command, String[] cmdComponents) {
        print("Invalid argument for '" + command + "' command");
        LOG.info("Invalid argument. " + Arrays.toString(cmdComponents));
        printUsage(command);
        return false;
    }
}
