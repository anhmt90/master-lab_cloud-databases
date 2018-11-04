package client.api;

import java.io.IOException;
import java.net.SocketTimeoutException;

import protocol.IMessage;

public interface IClient {

	/**
     * Creates a new client and opens a client socket to immediately connect to the server identified by the fields
     * address and port of Client
     */
    void connect() throws IOException;

    /**
     * Disconnects the connection to the server by closing the socket
     */
    void disconnect();

    /**
     * Sends the data provided as byte array to the connected server
     *
     * @param data The data to be sent to the connected server
     */
    void send(byte[] data) throws IOException;

    /**
     * Receives data sent from the connected server
     *
     * @return The byte array received from the connected server
     * @throws SocketTimeoutException
     */
    byte[] receive() throws SocketTimeoutException;

    /**
     * Check if the socket is closed or not
     *
     * @return boolean value indicating the client socket is closed or not
     */
    boolean isClosed();

    /**
     * Check if the client is still connected to the connected server host
     *
     * @return boolean value indicating the client is currently connected to any remote host
     */
    boolean isConnected();

    /**
     * Inserts a key-value pair into the StorageServer.
     *
     * @param key   the key that identifies the given value.
     * @param value the value that is indexed by the given key. if value is null
     * 				the value corresponding to the given key on the server is deleted instead
     * @return a server response that confirms the insertion of the tuple, an insertion error, 
     * 		   the deletion of the value on the server or a deletion error.
     */
    public IMessage put(String key, String value) throws IOException;

    /**
     * Retrieves the value for a given key from the StorageServer.
     *
     * @param key the key that identifies the value.
     * @return a server response, containing the value for the indexed key or an error.
     */
    public IMessage get(String key) throws IOException;
}
