package server.app;


public class ServerManager {


    public static void main(String[] args) {
        KVServer server = new KVServer(50000, 10, "FIFO" );
    }
}
