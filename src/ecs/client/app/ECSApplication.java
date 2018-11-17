package ecs.client.app;

import java.io.IOException;
import java.util.*;

import ecs.client.api.ECSClient;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import util.StringUtils;

public class ECSApplication {

	 	private static final String WHITESPACE = " ";
	    static Logger LOG = LogManager.getLogger("ECS");

	    private static final String INIT = "initiate";
	    private static final String START = "startService";
	    private static final String STOP = "stopService";
	    private static final String SHUTDOWN = "shutdown";
	    private static final String ADD = "add";
	    private static final String REMOVE = "remove";
	    private static final String HELP = "help";
	    private static final String QUIT = "quit";
	
	
	    /**
	     * The ECSClient as an instance of {@link Client} to communicate with the
	     * Storage Service
	     */
	    private static ECSClient ecsClient = new ECSClient();
	    
	    
	    public static void main(String[] args) throws IOException {
	        Scanner input = new Scanner(System.in);

	        while (true) {
	            printCommandPrompt();
	            String userInput = input.nextLine();
	            if(StringUtils.isEmpty(userInput))
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
	                    handleShutdown();
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
		private static void handleAddNode(String[] cmdComponents) {
			if (!isValidArgs(ADD, cmdComponents)) {
	            return;
	        }
			String[] cmdArgs = cmdComponents[1].split(" ");
			if(!isValidCacheSize(cmdArgs[0]) || !isValidDisplacementStrategy(cmdArgs[1])) {
				return;
			}
			ecsClient.addNode(Integer.parseInt(cmdArgs[0]), cmdArgs[1]);
		}


		/**
		 * Handles the command {@see SHUTDOWN}
		 */
		private static void handleShutdown() {
			ecsClient.shutDown();
		}


		/**
		 * Handles the command {@see STOP}
		 */
		private static void handleStop() {
//			if (ecsClient.isRunning()) {
//				print("Storage service is not currently running.");
//				return;
//			}
			ecsClient.stopService();
			
		}


		/**
		 * Handles the command {@see INIT}
		 * 
		 * @param cmdComponents User input separated by the first whitespace. The
		 *                      command name is the first component and the remaining as
		 *                      the second
		 */
		private static void handleInitiateService(String[] cmdComponents) {
			if (!isValidArgs(INIT, cmdComponents)) {
	            return;
	        }
			String[] cmdArgs = cmdComponents[1].split(" ");
			int serverNumber = 0;
			try {
				serverNumber = Integer.parseInt(cmdArgs[0]);
				if (serverNumber > 10 && serverNumber < 1) {
					String msg = "Not a valid number of servers.";
					print(msg);
		            LOG.info(msg);
		            return;
				}
			}
			catch (NumberFormatException e){
				String msg = "Not a valid number of servers.";
				print(msg);
	            LOG.info(msg);
	            return;
			}
			if(!isValidCacheSize(cmdArgs[1]) || !isValidDisplacementStrategy(cmdArgs[2])) {
				return;
			}
			ecsClient.initService(serverNumber, Integer.parseInt(cmdArgs[1]), cmdArgs[2]);
		}


		/**
		 * Handles the command {@see REMOVE}
		 */
		private static void handleRemoveNode() {
			ecsClient.removeNode();
			
		}


		/**
		 * Handles the command {@see START}
		 */
		private static void handleStart() {
			ecsClient.startService();
			
		}
		
		/**
	     * Handles the command {@see QUIT}
	     *
	     * @param input The scanner for input stream from System.in
	     */
	    private static void handleQuit(Scanner input) {
	        print("Exiting application");
	        LOG.info("quit");
	        //ecsClient.disconnect();
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
	                return "'startService' - starts service on all storage server instances participating in the service\n";
	            case STOP:
	                return "'stopService' - servers stopService processing client requests\n";
	            case INIT:
	                return "'initiate <numberOfNodes> <cacheSize> <displacementStrategy>' - starts the storage service with the given parameters\n";
	            case SHUTDOWN:
	                return "'shutdown' - stopService all servers and exit the remote process\n";
	            case ADD:
	                return "'add <cacheSize> <displacementStrategy> - create a storage server and add it to storage service at arbitrary position\n";
	            case REMOVE:
	            	return "'remove' - remove arbitrary node from storage service\n";
	            case HELP:
	                return "'help' - display list of commands\n";
	            case QUIT:
	                return "'quit' - end any ongoing connections and stopService the application\n";
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
	        System.out.print(output);
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
	                if (cmdComponents.length != 2 || cmdComponents[1].split(WHITESPACE).length != 3)
	                    return handleInvalidArgs(commandName, cmdComponents);
	                break;
	            case ADD:
	                if (cmdComponents.length != 2 || cmdComponents[1].split(WHITESPACE).length != 2)
	                    return handleInvalidArgs(commandName, cmdComponents);
	                break;
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
	        print("Invalid cache size. Cache Size has to be a number between 1 and 1073741824.");
	        return false;
	    }
	    
	    /**
	     * Checks if a String is a valid Displacement Strategy
	     *
	     * @param strategy Displacement Strategy in String format
	     * @return boolean indicating if {@param strategy} is a valid
	     *                 displacement strategy
	     */
	    private static boolean isValidDisplacementStrategy(String strategy) {
	        switch (strategy) {
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
