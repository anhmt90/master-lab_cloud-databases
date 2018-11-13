package ecs.client.app;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import ecs.client.api.ECSClient;
import protocol.IMessage;
import protocol.IMessage.Status;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import util.StringUtils;

public class ECSApplication {

	 private static final String WHITESPACE = " ";
	    static Logger LOG = LogManager.getLogger("ECS");

	    private static final String INIT = "initate";
	    private static final String START = "start";
	    private static final String STOP = "stop";
	    private static final String SHUTDOWN = "shutdown";
	    private static final String ADD = "add";
	    private static final String REMOVE = "remove";
	    private static final String HELP = "help";
	    private static final String QUIT = "quit";
	
	
	    /**
	     * The StorageClient as an instance of {@link Client} to communicate with the
	     * StorageServer
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


		private static void handleAddNode(String[] cmdComponents) {
			if (!isValidArgs(ADD, cmdComponents)) {
	            return;
	        }
			String[] cmdArgs = cmdComponents[1].split(" ");
			int cacheSize = Integer.parseInt(cmdArgs[0]);
			ecsClient.addNode(cacheSize, cmdArgs[1]);
		}


		private static void handleShutdown() {
			ecsClient.shutDown();
		}


		private static void handleStop() {
			ecsClient.stop();
			
		}


		private static void handleInitiateService(String[] cmdComponents) {
			if (!isValidArgs(INIT, cmdComponents)) {
	            return;
	        }
			String[] cmdArgs = cmdComponents[1].split(" ");
			int cacheSize = Integer.parseInt(cmdArgs[1]);
			int numberOfNodes = Integer.parseInt(cmdArgs[0]);
			ecsClient.initService(numberOfNodes, cacheSize, cmdArgs[2]);
		}


		private static void handleRemoveNode() {
			ecsClient.removeNode();
			
		}


		private static void handleStart() {
			ecsClient.start();
			
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
	                return "'start' - starts service on all storage server instances participating in the service\n";
	            case STOP:
	                return "'stop' - servers stop processing client requests\n";
	            case INIT:
	                return "'initiate <numberOfNodes> <cacheSize> <displacementStrategy>' - starts the storage service with the given parameters\n";
	            case SHUTDOWN:
	                return "'shutdown' - stop all servers and exit the remote process\n";
	            case ADD:
	                return "'add <cacheSize> <displacementStrategy> - create a storage server and add it to storage service at arbitrary position\n";
	            case REMOVE:
	            	return "'remove' - remove arbitrary node from storage service\n";
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
	                if (cmdComponents.length != 2 || cmdComponents[1].split(WHITESPACE).length > 2)
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
