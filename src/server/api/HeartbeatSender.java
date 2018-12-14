package server.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;

import management.FailureReportMessage;
import management.FailureStatus;
import management.ConfigMessageMarshaller;
import server.app.Server;


public class HeartbeatSender implements Runnable {

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private String successorAddress;
    private int successorPort;
    private Server server;

    private InetAddress address;
    private DatagramSocket socket;

    public HeartbeatSender(String successorAddress, int successorPort, Server server) {
        this.successorPort = successorPort;
        this.successorAddress = successorAddress;
        this.server = server;
    }

    public void run() {
        try {
            address = InetAddress.getByName(successorAddress);
            socket = new DatagramSocket();
            while (socket != null && !socket.isClosed() && address != null) {
                byte[] marshalledHeartbeat = ConfigMessageMarshaller.marshall(new FailureReportMessage(FailureStatus.HEARTBEAT));
                DatagramPacket packet = new DatagramPacket(marshalledHeartbeat, marshalledHeartbeat.length, address, successorPort);
                LOG.info("Sending heartbeat to <" + address + ":" + successorPort + ">");
                socket.send(packet);

                Thread.sleep(server.getHeartbeatInterval());
            }
        } catch (IOException ex) {
            LOG.error("Error during sending of heartbeat occured.");
        } catch (InterruptedException ex) {
            LOG.error("Heartbeat thread interrupted.");
        } finally {
            close();
        }

    }

    public void close() {
        socket.close();
        socket = null;
    }
}
