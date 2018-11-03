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

import protocol.*;
import protocol.IMessage.*;

public class Client implements IClient {

	private static Logger LOG = LogManager.getLogger(Client.class);
	/**
	 * The client socket
	 */
	Socket socket;

	/**
	 * The output stream for sending data to server
	 */
	BufferedOutputStream bos;

	/**
	 * the input stream for receiving data to server
	 */
	BufferedInputStream bis;

	/**
	 * Creates a new client and opens a client socket to immediately connect to the
	 * server identified by the parameters
	 *
	 * @param address The address of the server to connect to
	 * @param port    The port number that server is listening to
	 */
	public Client(String address, int port) {
		connect(address, port);
	}

	public Client() {

	}

	@Override
	public void connect(String address, int port) {
		try {
			socket = new Socket();
			socket.connect(new InetSocketAddress(address, port), 5000);
		} catch (UnknownHostException uhe) {
			print("Unknown host\n");
			LOG.error("Unknow Host");
			LOG.debug(uhe);
		} catch (SocketTimeoutException e) {
			print("Could not connect to server. Connection time out.\n");
			LOG.error("Connection timed out.");
		} catch (IOException e) {
			print("Could not connect to server.\n");
			LOG.error("Couldn't connect to the server");
			LOG.debug(e);
		}
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
			print("Connection is already inactive.\n");
			LOG.error("Connection is already inactive.");
			LOG.debug(e);
		}
	}

	@Override
	public void send(byte[] data) {
		try {
			bos = new BufferedOutputStream(socket.getOutputStream());
			bos.write(data);
			bos.flush();
			LOG.info("sending " + data.length + " bytes to server");
		} catch (IOException e) {
			print("Could't connect to the server. Disconnecting...\n");
			LOG.error("Could't connect to the server. Disconnecting...");
			LOG.debug(e);
			disconnect();
		}
	}

	@Override
	public byte[] receive() {
		byte[] data = new byte[1024 * 128];
		try {
			socket.setSoTimeout(5000);
			bis = new BufferedInputStream(socket.getInputStream());
			int bytesCopied = bis.read(data);
			LOG.info("received data from server");
		} catch (SocketTimeoutException ste) {
			print("'Receive' timed out. Client will disconnect from server.\n");
			LOG.error("'Receive' timed out. Client will disconnect from server.");
			LOG.debug(ste);
			disconnect();
		} catch (IOException e) {
			print("Could't connect to the server. Disconnecting...\n");
			LOG.error("Could't connect to the server. Disconnecting...");
			LOG.debug(e);
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
	public IMessage put(String key, String value) {
		if (value != null) {
			byte[] keySend = key.getBytes(StandardCharsets.US_ASCII);
			byte[] valueSend = value.getBytes(StandardCharsets.US_ASCII);
			IMessage put = new Message(Status.PUT, new K(keySend), new V(valueSend));
			send(MessageMarshaller.marshall(put));
			IMessage response = MessageMarshaller.unmarshall(receive());
			LOG.debug(response);
			return response;
		} else {
			byte[] keySend = key.getBytes(StandardCharsets.US_ASCII);
			IMessage delete = new Message(Status.DELETE, new K(keySend));
			send(MessageMarshaller.marshall(delete));
			IMessage response = MessageMarshaller.unmarshall(receive());
			LOG.debug(response);
			return response;
		}
	}

	@Override
	public IMessage get(String key) {
		byte[] keySend = key.getBytes(StandardCharsets.US_ASCII);
		IMessage put = new Message(Status.GET, new K(keySend));
		send(MessageMarshaller.marshall(put));
		IMessage response = MessageMarshaller.unmarshall(receive());
		LOG.debug(response);
		return response;
	}

}
