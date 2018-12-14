package ecs;

import management.FailureReportMessage;
import management.ConfigMessageMarshaller;
import management.FailureStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;

import static protocol.IMessage.MAX_MESSAGE_LENGTH;

public class FailureReportConnection implements Runnable {

    private static Logger LOG = LogManager.getLogger("ECS");
    private final FailureReportingManager manager;
    private boolean isOpen;

    private Socket peer;

    private ExternalConfigurationService ecs;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    public FailureReportConnection(FailureReportingManager manager, Socket peer, ExternalConfigurationService ecs) {
        this.manager = manager;
        this.peer = peer;
        this.ecs = ecs;
        isOpen = true;
    }

    /**
     * Listens for eventual failure reports from storage servers
     */
    @Override
    public void run() {
        try {
            while (isOpen && ecs.isRingUp()) {
                FailureReportMessage failureMessage = poll();
                boolean success = handleReport(failureMessage);

                FailureReportMessage ack = success != false ? new FailureReportMessage(FailureStatus.FAILURE_RESOLVED) : new FailureReportMessage(FailureStatus.FAILURE_UNRESOLVED);
                LOG.info("sending ACK " + ack.getStatus());
                send(ack);

            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection lost", ioe);
            isOpen = false;
        } finally {
            try {
                close();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    void close() throws IOException {
        boolean success = manager.getConnectionTable().remove(this);
        if (peer != null) {
            LOG.warn("remove success=" + success + " closing connection to " + peer.toString());
            bis.close();
            bos.close();
            peer.close();
            bis = null;
            bos = null;
            peer = null;
        }
    }

    /**
     * calls relevant functions based on the server request
     *
     * @param failureMessage the message containing server request
     * @return
     */
    private boolean handleReport(FailureReportMessage failureMessage) {
        LOG.info("Handling potential server outage report from " + peer.getInetAddress().getHostAddress() + ":" + peer.getPort());
        switch (failureMessage.getStatus()) {
            case SERVER_FAILURE:
                return ecs.handleFailure(failureMessage.getTargetServer().getRange());
            default:
                throw new IllegalStateException("Unknown failure report!");
        }
    }


    /**
     * Sends out a FailureReportMessage
     *
     * @param message Message that is sent
     * @throws IOException
     */
    public void send(FailureReportMessage message) throws IOException {
        bos = new BufferedOutputStream(peer.getOutputStream());
        byte[] bytes = ConfigMessageMarshaller.marshall(message);
        bos.write(bytes);
        bos.flush();

        LOG.info("SEND \t<"
                + peer.getInetAddress().getHostAddress() + ":"
                + peer.getPort() + ">: '"
                + message.toString() + "'");
    }

    /**
     * Receives a failure report by a storage server
     *
     * @return the received message
     * @throws IOException
     */
    private FailureReportMessage poll() throws IOException {
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        while (true) {
            try {
                bis = new BufferedInputStream(peer.getInputStream());
                int justRead = bis.read(messageBuffer);
                FailureReportMessage message = ConfigMessageMarshaller.unmarshallFailureReportMessage(Arrays.copyOfRange(messageBuffer, 0, justRead));

                LOG.info("RECEIVE \t<"
                        + peer.getInetAddress().getHostAddress() + ":"
                        + peer.getPort() + ">: '"
                        + message.toString().trim() + "'");
                return message;
            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            }
        }
    }

    public void setOpen(boolean open) {
        isOpen = open;
    }

    public boolean isOpen() {
        return isOpen;
    }
}
