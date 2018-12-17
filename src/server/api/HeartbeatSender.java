package server.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;

import management.FailureReportMessage;
import management.ReportStatus;
import management.ConfigMessageMarshaller;
import server.app.Server;


public class HeartbeatSender implements Runnable {

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private String successorAddress;
    private int successorPort;

    private InetAddress address;
    private DatagramSocket heartbeatSocket;

    public HeartbeatSender(String successorAddress, int successorPort, Server server) {
        this.successorPort = successorPort;
        this.successorAddress = successorAddress;
    }

    public void run() {
        try {
            address = InetAddress.getByName(successorAddress);
            heartbeatSocket = new DatagramSocket();
            while (heartbeatSocket != null && !heartbeatSocket.isClosed() && address != null) {
                byte[] marshalledHeartbeat = ConfigMessageMarshaller.marshall(new FailureReportMessage(ReportStatus.HEARTBEAT));
                DatagramPacket packet = new DatagramPacket(marshalledHeartbeat, marshalledHeartbeat.length, address, successorPort);
                LOG.info("Sending heartbeat to <" + address + ":" + successorPort + ">");
                heartbeatSocket.send(packet);

                Thread.sleep(Server.HEARTBEAT_INTERVAL);
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
        if(heartbeatSocket != null)
            heartbeatSocket.close();
    }
}
