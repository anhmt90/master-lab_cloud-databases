package mapreduce.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.Constants;
import server.app.Server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;

import static protocol.Constants.MAX_TASK_MESSAGE_LENGTH;
import static protocol.Constants.MR_TASK_HANDLER_PORT_DISTANCE;

public class TaskReceiver implements Runnable {
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);
    private int port;
    private Server server;

    private DatagramSocket taskInboundSocket;

    public TaskReceiver(Server server) {
        this.server = server;
        this.port = server.getServicePort() + Constants.MR_TASK_RECEIVER_PORT_DISTANCE;
    }

    @Override
    public void run() {
        try {
            LOG.info("Creating taskInboundSocket on port " + port);
            taskInboundSocket = new DatagramSocket(port);
            while (server.isRunning()) {
                DatagramPacket taskPacket = new DatagramPacket(new byte[MAX_TASK_MESSAGE_LENGTH], MAX_TASK_MESSAGE_LENGTH);
                taskInboundSocket.receive(taskPacket);

                TaskHandler taskHandler = new TaskHandler(taskPacket, server);
                new Thread(taskHandler).start();
                LOG.info("TaskHandler started. Handling task ....");
            }
        } catch (IOException e) {
            LOG.error(e);
        }

    }

    /**
     * closes the socket and stop receiving MR tasks
     */
    public void close() {
        if(taskInboundSocket != null)
            taskInboundSocket.close();
    }
}
