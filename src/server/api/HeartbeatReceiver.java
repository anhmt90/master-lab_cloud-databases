package server.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.DatagramSocket;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketTimeoutException;

import management.FailureReportMessage;
import management.ReportStatus;
import management.ConfigMessageMarshaller;
import server.app.Server;

/**
 * Receives heartbeat messages from other servers in the storage service and upon detecting missing heartbeats calls failure handling
 * 
 */
public class HeartbeatReceiver implements Runnable {

    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private int receiverPort;
    private Server server;

    private DatagramSocket heartbeatSocket;

    public HeartbeatReceiver(int receiverPort, Server server) {
        this.receiverPort = receiverPort;
        this.server = server;
    }

    /**
     * opens a UDP socket that listens on a designated port and upon missing 5 consecutive heartbeat calls failure detection on the associated server
     */
    public void run() {
        try {
            int missedHeartbeats = 0;
            byte[] marshalledHeartbeat = ConfigMessageMarshaller.marshall(new FailureReportMessage(ReportStatus.HEARTBEAT));
            int heartbeatMessageLength = marshalledHeartbeat.length;

            heartbeatSocket = new DatagramSocket(receiverPort);
            heartbeatSocket.setSoTimeout(2 * Server.HEARTBEAT_INTERVAL);

            while (heartbeatSocket != null && !heartbeatSocket.isClosed()) {
                if (missedHeartbeats > 4) {
                    reportFailure();
                    return;
                }

                DatagramPacket heartbeat = new DatagramPacket(new byte[heartbeatMessageLength], heartbeatMessageLength);
                try {
                    heartbeatSocket.receive(heartbeat);
                    byte[] receivedHeartbeat = heartbeat.getData();
                    FailureReportMessage heartbeatMessage = ConfigMessageMarshaller.unmarshallFailureReportMessage(receivedHeartbeat);
                    if (heartbeatMessage.getStatus() == ReportStatus.HEARTBEAT) {
                        LOG.debug("Received heartbeat from <" + heartbeat.getAddress() + ":" + heartbeatSocket.getPort() + ">" + heartbeat.getSocketAddress());
                        missedHeartbeats = 0;
                    }
                } catch (SocketTimeoutException ex) {
                    missedHeartbeats++;
                    LOG.warn("Didn't receive heartbeat. Timeout " + missedHeartbeats + " times");
                }

            }
        } catch (IOException ex) {
            LOG.error("Error trying to receive heartbeat.");
        } finally {
            close();
        }
    }

    /*******************************************************************************************************/
    private FailureReporter reporter;
    

    /**
     * reports a failure to the ECS
     */
    public void reportFailure() {
        LOG.info("Detect failure! File a report to ECS...");
        try {
            reporter = new FailureReporter();
        } catch (IOException ex) {
            LOG.error("Couldn't establish connection to the ECS for failure reporting.");
        }

        if (reporter != null) {
            LOG.info("sending failure report");
            boolean success = reporter.sendFailureReport(server.getMetadata().getPredecessor(server.getWriteRange()));
            LOG.info(success ? "Report successfully sent" : "Fail to send report");
        } else {
            LOG.info("Predecessor failure detected but unable to notify ECS about it.");
        }
    }

    /**
     * Closes the socket and ends the service
     */
    public void close() {
        if(heartbeatSocket != null)
            heartbeatSocket.close();
    }
}
