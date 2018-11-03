package client.app;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.net.SocketTimeoutException;

import client.api.Client;
import protocol.Message;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;

/**
 *
 */
public class CommandLineApp {

	static Logger LOG = LogManager.getLogger(CommandLineApp.class);

	private static final int MAX_VALUE_SIZE = (120 * 1024) - 1;
	private static final int MAX_KEY_LENGTH = 20;
	private static final String CONNECT = "connect";
	private static final String DISCONNECT = "disconnect";
	private static final String PUT = "put";
	private static final String GET = "get";
	private static final String LOG_LEVEL = "logLevel";
	private static final String HELP = "help";
	private static final String QUIT = "quit";

	/**
	 * The EchoClient as an instance of {@link Client} to communicate with the
	 * EchoServer
	 */
	private static Client echoClient = new Client();

	/**
	 * Gets user inputs and calls the respective methods to handle the input
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		Scanner input = new Scanner(System.in);

		while (true) {
			// Test if input is in an acceptable format and then split so you can parse the
			// command
			printCommandPrompt();
			String userInput = input.nextLine();
			LOG.debug("User input: " + userInput);
			String[] cmdComponents = userInput.split(" ", 2);
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
			LOG.debug(msg);
		}
	}

	/**
	 * Handles the command {@see QUIT}
	 *
	 * @param input The scanner for input stream from System.in
	 */
	private static void handleQuit(Scanner input) {
		print("Exiting application");
		LOG.debug("quit");
		echoClient.disconnect();
		input.close();
	}

	/**
	 * Handles the command {@see PUT}
	 *
	 * @param cmdComponents User input separated by the first whitespace. The
	 *                      command name is the first component and the remaining as
	 *                      the second
	 */
	private static void handlePut(String[] cmdComponents) {
		if (!isClientConnected())
			return;

		if (!isValidArgs(PUT, cmdComponents)) {
			return;
		}
		String[] keyValue = cmdComponents[1].split(" ", 2);
		if (keyValue[0].length() < 1) {
			print("Key needs to be at least one non whitespace character.");
			LOG.debug("Invalid argument. " + cmdComponents);
			return;
		}
		String key = keyValue[0];
		if (key.length() > MAX_KEY_LENGTH) {
			String msg = "key exceeds maximum length";
			print(msg);
			LOG.debug(msg + ": " + key.length());
			return;
		}

		if (keyValue.length != 2) {
			handleDelete(key);
			return;
		} else if (keyValue[1].length() < 1) {
			handleDelete(key);
			return;
		}

		String value = keyValue[1];

		if (value.length() > MAX_VALUE_SIZE) {
			String msg = "value exceeds maximum size";
			print(msg);
			LOG.debug(msg + ": " + value.length());
			return;
		}

		if (!echoClient.isClosed()) {
			LOG.debug("Putting: ", key, value);

			Message serverResponse = (Message) echoClient.put(key, value);
			switch (serverResponse.getStatus()) {
			case PUT_ERROR:
				print("Storing key-value pair unsuccessful.");
				LOG.debug("Server response: PUT_ERROR");
				break;
			case DELETE_SUCCESS:
				print("Stored key-value pair on server.");
				LOG.debug("Server response: PUT_SUCCESS");
				break;
			default:
				print("Wrong server response. Please try again");
				LOG.debug("Incompatible server response", serverResponse);
			}
		} else {
			print("No connection currently established. Please establish a connection before storing a key-value pair.");
		}
	}

	/**
	 * Handles deletion of values as a functionality of the put() command
	 *
	 * @param key Key for which the corresponding value will be deleted            
	 */
	private static void handleDelete(String key) {
		if (!echoClient.isClosed()) {
			LOG.debug("Deleting: ", key);

			Message serverResponse = (Message) echoClient.put(key, null);
			switch (serverResponse.getStatus()) {
			case DELETE_ERROR:
				print("Deletion unsuccessful. No value for key " + key + " might be stored on server.");
				LOG.debug("Server response: DELETE_ERROR");
				break;
			case DELETE_SUCCESS:
				print("Value stored on server for key " + key + " was deleted.");
				LOG.debug("Server response: DELETE_SUCCESS");
				break;
			default:
				print("Wrong server response. Please try again.");
				LOG.debug("Incompatible server response", serverResponse);
			}
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
	private static void handleGet(String[] cmdComponents) {
		if (!isClientConnected())
			return;

		if (!isValidArgs(GET, cmdComponents)) {
			return;
		}
		String key = cmdComponents[1];
		if (key.length() > MAX_KEY_LENGTH) {
			String msg = "Key exceeds maximum length";
			print(msg);
			LOG.debug(msg + ": " + key.length());
			return;
		}
		if (!echoClient.isClosed()) {
			LOG.debug("Getting: ", key);

			Message serverResponse = (Message) echoClient.get(key);
			switch (serverResponse.getStatus()) {
			case GET_ERROR:
				print("Retrieving value was unsuccesful. There might be no value saved on the server corresponding to the given key: "
						+ key);
				LOG.debug("Server response: GET_ERROR");
				break;
			case GET_SUCCESS:
				print("Value stored on server for key " + key + " is: " + serverResponse.getKey().get());
				LOG.debug("Server response: GET_SUCCESS");
				break;
			default:
				print("Wrong server response. Please try again");
				LOG.debug("Incompatible server response", serverResponse);
			}
		} else {
			print("No connection currently established. Please establish a connection before retrieving a value");
		}
	}

	/**
	 * Disconnects the connection to EchoServer
	 */
	private static void handleDisconnect() {
		if (!isClientConnected())
			return;
		echoClient.disconnect();
		String msg = "Echo client disconnected.";
		print(msg);
		LOG.debug(msg);

	}

	/**
	 * Handles the command {@see CONNECT}
	 *
	 * @param cmdComponents User input separated by the first whitespace. The
	 *                      command name is the first component and the remaining as
	 *                      the second
	 */
	private static void handleConnect(String[] cmdComponents) {
		if (!isValidArgs(CONNECT, cmdComponents)) {
			return;
		}

		if (echoClient != null && echoClient.isConnected() && !echoClient.isClosed()) {
			String msg = "Client is already connected to a server. Please disconnect before establishing another connection.";
			print(msg);
			LOG.debug(msg);
			return;
		}
		String commandArgs = cmdComponents[1];
		String[] connectionInfo = commandArgs.split(" ");
		if (!isValidArgs(CONNECT, connectionInfo) || !isValidPortNumber(connectionInfo[1]))
			return;

		String address = connectionInfo[0];
		int port = Integer.parseInt(connectionInfo[1]);

		echoClient = new Client(address, port);

		if (echoClient.isConnected()) {
			print(receiveMessageFromServer());
		}
	}

	/**
	 * Receives and format message from EchoServer
	 *
	 * @return The message received from EchoServer in string format
	 */
	private static String receiveMessageFromServer() {
		byte[] bytes = echoClient.receive();
		String message = new String(bytes, StandardCharsets.US_ASCII).trim();
		LOG.debug("Bytes from server: ", bytes);
		LOG.debug("Message from server: " + message);
		return message;
	}

	/**
	 * Prints help text giving reference for each command and purpose of the
	 * application
	 */
	public static void printHelp() {
		print("This application works as an echo client. The command set is as follows:\n" + getUsage(CONNECT)
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
	 * Prints the command prompt 'EchoClient>' to System.out
	 */
	private static void printCommandPrompt() {
		System.out.print("\nEchoClient> ");
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
	 * @return boolean value indicating the EchoClient is still connected with the
	 *         EchoServer or not
	 */
	private static boolean isClientConnected() {
		if (echoClient.isClosed() || !echoClient.isConnected()) {
			String msg = "Client is not currently connected to server.";
			print(msg);
			LOG.debug(msg);
			return false;
		}
		return true;
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
	 *         {@see commandName} are valid or not
	 */
	private static boolean isValidArgs(String commandName, String[] cmdComponents) {
		if (cmdComponents.length != 2) {
			print("Invalid argument for '" + commandName + "' command");
			LOG.debug("Invalid argument. " + cmdComponents);
			printUsage(commandName);
			return false;
		}
		return true;
	}
}
