package ecs;

import management.FailureReportMessage;
import management.ConfigMessageMarshaller;
import management.ReportStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;

import static ecs.FailureReportPortal.FAILURE_LOG;
import static protocol.IMessage.MAX_MESSAGE_LENGTH;

/**
 * Responsible for receiving failure reports from servers in the storage service
 *
 */
public class ReporterConnection implements Runnable {
    private static Logger LOG = LogManager.getLogger(FAILURE_LOG);

    private static final int MAX_ALLOWED_EOF = 3;

    private final FailureReportPortal manager;

    private Socket serverSocket;

    private ExternalConfigurationService ecs;
    private BufferedInputStream bis;
    private BufferedOutputStream bos;

    public ReporterConnection(FailureReportPortal manager, Socket serverSocket, ExternalConfigurationService ecs) {
        this.manager = manager;
        this.serverSocket = serverSocket;
        this.ecs = ecs;
    }

    /**
     * Listens for eventual failure reports from storage servers
     */
    @Override
    public void run() {
        int eofCounter = 0;
        try {
            while (true) {
                FailureReportMessage failureMessage = poll();
                if (failureMessage == null) {
                    eofCounter++;
                    if (eofCounter >= MAX_ALLOWED_EOF) {
                        LOG.warn("Got " + eofCounter + " successive EOF signals! Assume the other end has terminated but not closed the socket properly. " +
                                "Tear down connection now");
                        break;
                    }
                    continue;
                }
                handleReport(failureMessage);
                break;
            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection lost", ioe);
        } catch (Exception e) {
            LOG.error("Runtime exception!", e);
        } finally {
            try {
                close();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    /**
     * Closes sockets and streams to end the connection
     * @throws IOException
     */
    public void close() throws IOException {
        boolean success = manager.getConnectionTable().remove(this);
        if (bis != null)
            bis.close();
        if (bos != null)
            bos.close();
        if (serverSocket != null) {
            LOG.warn("remove success=" + success + ". Closing connection to " + serverSocket.getInetAddress());
            serverSocket.close();
        }
        bis = null;
        bos = null;
        serverSocket = null;
    }

    /**
     * calls relevant functions based on the server request
     *
     * @param failureMessage the message containing serverSocket request
     */
    private void handleReport(FailureReportMessage failureMessage) throws IOException {
        LOG.info("Handling potential server outage report from " + serverSocket.getInetAddress().getHostAddress() + ":" + serverSocket.getPort());
        switch (failureMessage.getStatus()) {
            case SERVER_FAILED:
                confirmReportReceived();
                ecs.handleFailure(failureMessage.getFailedServer().getWriteRange());
                break;
            default:
                throw new IllegalStateException("Unknown report status!");
        }
    }

    private void confirmReportReceived() throws IOException {
        LOG.info("sending ACK " + ReportStatus.REPORT_RECEIVED);
        send(new FailureReportMessage(ReportStatus.REPORT_RECEIVED));
    }


    /**
     * Sends out a FailureReportMessage
     *
     * @param message Message that is sent
     * @throws IOException
     */
    public void send(FailureReportMessage message) throws IOException {
        bos = new BufferedOutputStream(serverSocket.getOutputStream());
        byte[] bytes = ConfigMessageMarshaller.marshall(message);
        bos.write(bytes);
        bos.flush();

        LOG.info("SEND \t<"
                + serverSocket.getInetAddress().getHostAddress() + ":"
                + serverSocket.getPort() + ">: '"
                + message.toString() + "'");
    }

    /**
     * Receives a failure report message by a storage server
     *
     * @return the received message
     * @throws IOException
     */
    private FailureReportMessage poll() throws IOException {
        byte[] messageBuffer = new byte[MAX_MESSAGE_LENGTH];
        while (true) {
            try {
                bis = new BufferedInputStream(serverSocket.getInputStream());
                int justRead = bis.read(messageBuffer);
                if (justRead < 0)
                    return null;

                FailureReportMessage message = ConfigMessageMarshaller.unmarshallFailureReportMessage(Arrays.copyOfRange(messageBuffer, 0, justRead));

                LOG.info("RECEIVE \t<"
                        + serverSocket.getInetAddress().getHostAddress() + ":"
                        + serverSocket.getPort() + ">: '"
                        + message.toString().trim() + "'");
                return message;
            } catch (EOFException e) {
                LOG.error("CATCH EOFException", e);
            }
        }
    }
}
