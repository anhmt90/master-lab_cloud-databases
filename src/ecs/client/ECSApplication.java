package ecs.client;

import java.io.IOException;
import java.util.*;

import ecs.ExternalConfigurationService;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import util.StringUtils;

import static util.FileUtils.SEP;

public class ECSApplication {
    static Logger LOG = LogManager.getLogger("ECS");
    private static final String CONFIG_FILE = System.getProperty("user.dir") + SEP + "config" + SEP + "server-info";

    private static final String INIT = "init";
    private static final String START = "start";
    private static final String STOP = "stop";
    private static final String SHUTDOWN = "shutdown";
    private static final String ADD = "add";
    private static final String REMOVE = "remove";
    private static final String HELP = "help";
    private static final String QUIT = "quit";


    /**
     * The ECSClient as an instance of {@link ExternalConfigurationService} to communicate with the
     * Storage Service
     */
    private static ExternalConfigurationService ecs;


    public static void main(String[] args) throws Exception {
        Scanner input = new Scanner(System.in);

        if (args == null || args.length > 0)
            ecs = new ExternalConfigurationService(args[0]);
        else
            ecs = new ExternalConfigurationService(CONFIG_FILE);

        while (true) {
            printCommandPrompt();
            String userInput = input.nextLine();
            if (StringUtils.isEmpty(userInput))
                continue;
            LOG.info("User input: " + userInput);
            String[] cmdComponents = userInput.split(" ", 2);
            String commandName = cmdComponents[0];

            switch (commandName) {
                case START:
                    handleStart();
                    break;
                case STOP:
                    handleStop();
                    break;
                case INIT:
                    handleInitiateService(cmdComponents);
                    break;
                case SHUTDOWN:
                    handleShutdown(cmdComponents);
                    break;
                case ADD:
                    handleAddNode(cmdComponents);
                    break;
                case REMOVE:
                    handleRemoveNode();
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


    /**
     * Handles the command {@see ADD}
     *
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second
     */
    private static void handleAddNode(String[] cmdComponents) throws IOException, InterruptedException {
        if (!isValidArgs(ADD, cmdComponents)) {
            return;
        }
        if (ecs.getPool().isEmpty()) {
        	print("No potential nodes remaining to add.");
        	return;
        }
        String[] cmdArgs = cmdComponents[1].split(" ");
        if (!isValidCacheSize(cmdArgs[0]) || !isValidDisplacementStrategy(cmdArgs[1])) {
            return;
        }
        try {
            ecs.addNode(Integer.parseInt(cmdArgs[0]), cmdArgs[1]);
            print("Add node successfully! The ring topology currently has " + ecs.getChord().size() + " nodes");
        } catch (RuntimeException e) {
            LOG.error(e);
            print("Failed to add node! Some error occurs: " + e.getMessage());
        }
    }


    /**
     * Handles the command {@see INIT}
     *
     * @param cmdComponents User input separated by the first whitespace. The
     *                      command name is the first component and the remaining as
     *                      the second
     */
    private static void handleInitiateService(String[] cmdComponents) throws Exception {
        if (!isValidArgs(INIT, cmdComponents)) {
            return;
        }
        String[] cmdArgs = cmdComponents[1].split(" ");
        int serverNumber = 0;
        try {
            serverNumber = Integer.parseInt(cmdArgs[0]);
            if (serverNumber > 256 || serverNumber < 3) {
                String msg = "Not a valid number of servers. Service needs at least 3 servers to guarantee replication safety.";
                print(msg);
                LOG.info(msg);
                return;
            }
        } catch (NumberFormatException e) {
            String msg = "Not a valid number of servers.";
            print(msg);
            LOG.info(msg);
            return;
        }
        if (!isValidCacheSize(cmdArgs[1]) || !isValidDisplacementStrategy(cmdArgs[2])) {
            return;
        }
        if (ecs.isRingUp() && ecs.getChord().size() > 0) {
            print("Storage service is already running.");
            return;
        }
        ecs.initService(serverNumber, Integer.parseInt(cmdArgs[1]), cmdArgs[2]);
        print(ecs.isRingUp() ? "Service initiated with " + serverNumber + " Servers." : "Initializing service failed");
    }


    /**
     * Handles the command {@see SHUTDOWN}
     *
     * @param cmdComponents
     */
    private static void handleShutdown(String[] cmdComponents) {
        if (ecs.isEmpty()) {
            print("No active nodes that could be shutdown.");
            return;
        }

        if (ecs.isRingUp()) {
            if (cmdComponents.length > 1) {
                handleShutdown(cmdComponents[1]);
                return;
            }
            ecs.shutdown();
            print("Storage service shut down successfully! The ring topology currently has " + ecs.getChord().size() + " nodes");
            LOG.info("Shutdown successfully!");
        } else {
            print("Storage service is currently not running.");
        }
    }

    private static void handleShutdown(String serverId) {
        print("Try to shut down server " + serverId);
        ecs.shutdown(serverId);
        print("Storage service shut down successfully! The ring topology currently has " + ecs.getChord().size() + " nodes");
    }


    /**
     * Handles the command {@see STOP}
     */
    private static void handleStop() {
        if (ecs.isEmpty()) {
            print("Service currently has no active nodes.");
            return;
        }
        if (!ecs.isServing()) {
            print("Couldn't stop service! Storage service is already stopped.");
            return;
        }
        ecs.stopService();
        print("Storage service stopped");
    }

    /**
     * Handles the command {@see REMOVE}
     */
    private static void handleRemoveNode() {
        if (ecs.getChord().size() < 4) {
            print("Due to replication safety no more nodes can be removed. The ring must have at least 3 nodes. " +
                    "Current number of nodes is " + ecs.getChord().size());
            return;
        }
        try {
            ecs.removeNode();
            print("Remove a random node successfully! The ring topology currently has " + ecs.getChord().size() + " nodes");
        } catch (RuntimeException e) {
            LOG.error(e);
            print("Failed to remove node! Some error occurs: " + e.getMessage());
        }

    }


    /**
     * Handles the command {@see START}
     */
    private static void handleStart() {
        if (ecs.isEmpty()) {
            print("Service currently has no active nodes.");
            return;
        }
        if (ecs.isServing()) {
            print("Couldn't start the service! Storage service has been already started.");
            return;
        }
        ecs.startService();
        print("Storage service started.");
    }

    /**
     * Handles the command {@see QUIT}
     *
     * @param input The scanner for input stream from System.in
     */
    private static void handleQuit(Scanner input) throws IOException {
        print("Exiting application. Bye!");
        LOG.info("quit");
        ecs.shutdown();
        ecs.getReportManager().getReportSocket().close();
        input.close();
    }

    /**
     * Prints help text giving reference for each command and purpose of the
     * application
     */
    public static void printHelp() {
        print("This application works as an external configuration service. The command set is as follows:\n" + getUsage(START)
                + getUsage(STOP) + getUsage(INIT) + getUsage(SHUTDOWN) + getUsage(ADD) + getUsage(REMOVE)
                + getUsage(HELP) + getUsage(QUIT)

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
            case START:
                return "'" + START + "' - starts service on all storage server instances participating in the service\n";
            case STOP:
                return "'" + STOP + "' - servers stop processing client requests\n";
            case INIT:
                return "'" + INIT + "' <numberOfNodes> <cacheSize> <displacementStrategy>' - starts the storage service with the given parameters\n";
            case SHUTDOWN:
                return "'" + SHUTDOWN + "' - stop all servers and exit the remote process\n";
            case ADD:
                return "'" + ADD + "' <cacheSize> <displacementStrategy> - create a storage server and add it to storage service at arbitrary position\n";
            case REMOVE:
                return "'" + REMOVE + "' - remove arbitrary node from storage service\n";
            case HELP:
                return "'" + HELP + "'- display list of commands\n";
            case QUIT:
                return "'" + QUIT + "' - end any ongoing connections and stop the application\n";
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
     * Prints the command prompt 'ECSClient>' to System.out
     */
    private static void printCommandPrompt() {
        System.out.print("\nECSClient> ");
    }

    /**
     * Prints an output string to System.out
     *
     * @param output The output string to print to System.out
     */
    private static void print(String output) {
        System.out.println(output);
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
            case INIT:
                if (cmdComponents.length != 2 || cmdComponents[1].split(StringUtils.WHITE_SPACE).length != 3)
                    return handleInvalidArgs(commandName, cmdComponents);
                break;
            case ADD:
                if (cmdComponents.length != 2 || cmdComponents[1].split(StringUtils.WHITE_SPACE).length != 2)
                    return handleInvalidArgs(commandName, cmdComponents);
                break;
        }
        return true;
    }

    /**
     * Checks whether the {@param cacheSizeString} is a valid cache loadedDataSize
     *
     * @param cacheSizeString The cache loadedDataSize number in string format
     * @return boolean value indicating the {@param cacheSizeString} is a valid cache
     * loadedDataSize or not
     */
    private static boolean isValidCacheSize(String cacheSizeString) {
        try {
            int cacheSizeInt = Integer.parseInt(cacheSizeString);
            if (cacheSizeInt > 1 && cacheSizeInt < 1073741824) {
                return true;
            }
        } catch (NumberFormatException nex) {

        }
        print("Invalid cache loadedDataSize. Cache Size has to be a number between 1 and 1073741824.");
        return false;
    }

    /**
     * Checks if a String is a valid Displacement Strategy
     *
     * @param strategy Displacement Strategy in String format
     * @return boolean indicating if {@param strategy} is a valid
     * displacement strategy
     */
    private static boolean isValidDisplacementStrategy(String strategy) {
        switch (strategy.toUpperCase()) {
            case "FIFO":
                return true;
            case "LRU":
                return true;
            case "LFU":
                return true;
            default:
                print("Illegal Displacement Strategy. Please choose either 'FIFO', 'LRU' or 'LFU'.");
                return false;
        }
    }

    /**
     * prints to console and log if user provided illegal arguments
     *
     * @param commandName   name of the command for which the user
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
