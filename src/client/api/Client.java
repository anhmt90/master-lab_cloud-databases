package client.api;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import ecs.Metadata;
import protocol.*;
import protocol.IMessage.*;
import util.HashUtils;

public class Client implements IClient {
	public static final String CLIENT_LOG = "kvClient";
	private static Logger LOG = LogManager.getLogger(Client.CLIENT_LOG);
	/**
	 * The client socket
	 */
	private Socket socket;

	/**
	 * The output stream for sending data to server
	 */
	private BufferedOutputStream bos;

	/**
	 * the input stream for receiving data to server
	 */
	private BufferedInputStream bis;

	private String address;
	private int port;

	private static final String PUT = "put";
	private static final String GET = "get";

	/**
	 * List of the storage servers with their addresses
	 */
	private Metadata metadata;

	/**
	 * Creates a new client and opens a client socket to immediately connect to the
	 * server identified by the parameters
	 *
	 * @param address The address of the server to connect to
	 * @param port    The port number that server is listening to
	 */
	public Client(String address, int port) {
		this.address = address;
		this.port = port;
	}

	public Client() {

	}

	@Override
	public void connect() throws IOException {
		try {
			socket = new Socket();
			socket.connect(new InetSocketAddress(address, port), 5000);
		} catch (UnknownHostException uhe) {
			throw printLogError(uhe, "Unknown host");
		} catch (SocketTimeoutException ste) {
			throw printLogError(ste, "Could not connect to server. Connection timeout.");
		} catch (IOException ioe) {
			throw printLogError(ioe, "Could not connect to server.");
		}
	}

	/**
	 * Logging of errors
	 * 
	 * @param exception the exception that was thrown in the error
	 * @param message   the error message
	 * @return exception
	 */
	private <E> E printLogError(E exception, String message) {
		print(message + "\n");
		LOG.error(message, exception);
		return exception;
	}

	@Override
	public void disconnect() {
		try {
			if (bos != null)
				bos.close();
			if (bis != null)
				bis.close();
			if (socket != null) {
				socket.close();
			}
			socket = new Socket();
		} catch (IOException e) {
			printLogError(e, "Connection is already closed.");
		}
	}

	@Override
	public void send(byte[] data) throws IOException {
		try {
			bos = new BufferedOutputStream(socket.getOutputStream());
			bos.write(data);
			bos.flush();
			LOG.info("sending " + data.length + " bytes to server");
		} catch (IOException e) {
			disconnect();
			throw printLogError(e, "Could't connect to the server. Disconnecting...");
		}
	}

	@Override
	public byte[] receive() {
		byte[] data = new byte[2 + 20 + 1024 * 120];
		try {
			socket.setSoTimeout(50000);
			bis = new BufferedInputStream(socket.getInputStream());
			int bytesCopied = bis.read(data);
			LOG.info("received data from server" + bytesCopied + " bytes");
		} catch (SocketTimeoutException ste) {
			printLogError(ste, "'receive' timeout. Client will disconnect from server.");
			disconnect();
		} catch (IOException e) {
			printLogError(e, "Could't connect to the server. Disconnecting...");
			disconnect();
		}
		return data;
	}

	@Override
	public boolean isClosed() {
		LOG.debug(socket);
		if (socket != null)
			LOG.debug(socket.isClosed());
		return socket == null || socket.isClosed();
	}

	@Override
	public boolean isConnected() {
		if (socket != null)
			return socket.isConnected();
		else
			return false;
	}

	/**
	 * Prints an output string to System.out
	 *
	 * @param output The output string to print to System.out
	 */
	private static void print(String output) {
		System.out.print(output);
	}

	@Override
	public IMessage put(String key, String value) throws IOException {
		IMessage serverResponse;
		if (value != null && value.equals("null"))
			value = null;
		if (value != null) {
			serverResponse = storeOnServer(key, value);
		} else {
			serverResponse = removeOnServer(key);
		}

		if (serverResponse.getStatus() == Status.SERVER_NOT_RESPONSIBLE) {
			serverResponse = handleServerMiss(key, value, serverResponse, PUT);
		}
		return serverResponse;
	}

	/**
	 * Handles retrying an operation if it targeted the wrong server
	 * @param key used in the storage operation
	 * @param value used in the storage operation
	 * @param serverResponse the server response that indicated the server miss
	 * @param command the command in which the server miss occured
	 * @return the server response
	 * @throws IOException
	 */
	private IMessage handleServerMiss(String key, String value, IMessage serverResponse, String command)
			throws IOException {
		print("Server miss. Retrying.");
		this.metadata = serverResponse.getMetadata();
		String newAddress = metadata.findMatchingServer(HashUtils.getHash(key));
		String[] splitAddress = newAddress.split(" ");
		disconnect();
		this.address = splitAddress[0];
		this.port = Integer.parseInt(splitAddress[1]);
		connect();
		if (isConnected()) {
			byte[] bytes = receive();
			String message = new String(bytes, StandardCharsets.US_ASCII).trim();
			LOG.info("Message from server: " + message);
			print(message);
		}
		switch (command) {
		case PUT:
			return put(key, value);
		case GET:
			return get(key);
		default:
			return serverResponse;
		}

	}

	public IMessage put(String key) throws IOException {
		return put(key, null);
	}

	/**
	 * Intermediary method for deletion of key-value pairs on server
	 * 
	 * @param key key for the value that is supposed to be deleted
	 * @return the server response
	 * @throws IOException
	 */
	private IMessage removeOnServer(String key) throws IOException {
		return sendWithoutValue(key, Status.PUT);
	}

	@Override
	public IMessage get(String key) throws IOException {
		IMessage serverResponse = sendWithoutValue(key, Status.GET);
		if (serverResponse.getStatus() == Status.SERVER_NOT_RESPONSIBLE) {
			serverResponse = handleServerMiss(key, null, serverResponse, GET);
		}
		return serverResponse;
	}

	/**
	 * Handles delivery of Messages without a value
	 * 
	 * @param key    key for the value that is accessed
	 * @param status message specification
	 * @return the server response
	 * @throws IOException
	 */
	private IMessage sendWithoutValue(String key, Status status) throws IOException {
		byte[] keyBytes = key.getBytes(StandardCharsets.US_ASCII);
		IMessage toSend = new Message(status, new K(keyBytes));
		send(MessageMarshaller.marshall(toSend));
		IMessage response = MessageMarshaller.unmarshall(receive());
		LOG.debug(response);
		return response;
	}

	/**
	 * Handles delivery of put messages for storage
	 * 
	 * @param key   key for the key-value pair
	 * @param value value for the key-value pair
	 * @return the server response
	 * @throws IOException
	 */
	private IMessage storeOnServer(String key, String value) throws IOException {
		byte[] keyBytes = key.getBytes(StandardCharsets.US_ASCII);
		byte[] valueBytes = value.getBytes(StandardCharsets.US_ASCII);
		IMessage toSend = new Message(Status.PUT, new K(keyBytes), new V(valueBytes));
		send(MessageMarshaller.marshall(toSend));
		IMessage response = MessageMarshaller.unmarshall(receive());
		LOG.info("Received from server: " + response);
		return response;
	}
}
