package client.api;


import protocol.IMessage;

public class Client implements IClient {


    /**
     * Initialize Client with address and port of Server
     *
     * @param address the address of the Server
     * @param port    the port of the Server
     */
    public Client(String address, int port) {

    }

    @Override
    public void connect() throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void disconnect() {
        // TODO Auto-generated method stub

    }

    @Override
    public IMessage put(String key, String value) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public IMessage get(String key) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }


}
