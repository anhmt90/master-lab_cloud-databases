package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;

/**
 * Establishes a failure report connection whenever a storage server wants to contact the ECS for a failure report
 * 
 */
public class FailureReportPortal implements Runnable {
    public static final String FAILURE_LOG = "failure_detection";
    private static Logger LOG = LogManager.getLogger(FAILURE_LOG);

    /**
     * socket to wait for failure report
     */
    private ServerSocket reportSocket;

    /**
     * the instance of {@link ExternalConfigurationService}
     */
    private ExternalConfigurationService ecs;

    /**
     * port number to listen for reports
     */
    private int port;

    /**
     * table containing all current running {@link ReporterConnection}
     */
    private HashSet<ReporterConnection> connectionTable;

    public FailureReportPortal(ExternalConfigurationService ecs) {
        this.ecs = ecs;
        port = ecs.getReportPort();
        connectionTable = new HashSet<>();
        init();
    }


    /**
     * initiates the server socket on the ecs to accept failure report connections
     */
    private void init() {
        LOG.info("Initialize reportSocket for internal management...");
        try {
            reportSocket = new ServerSocket(port);
            LOG.info("reportSocket created on port " + reportSocket.getLocalPort() + " for reporting");
        } catch (IOException e) {
            LOG.error("Error! Cannot poll server reportSocket:");
            if (e instanceof BindException) {
                LOG.error("Port " + port + " is already bound!");
            }
            e.printStackTrace();
        }
    }

    /**
     * waits for connection from storage server on which failures will be reported
     */
    @Override
    public void run() {
        try {
            LOG.info("running = " + ecs.isRingUp());
            while (true && reportSocket != null) {
                LOG.info("Report portal is waiting for incoming connection on port " + reportSocket.getLocalPort() + " for failure report");
                Socket peer = reportSocket.accept();
                ReporterConnection failureConnection = new ReporterConnection(this, peer, ecs);
                connectionTable.add(failureConnection);
                new Thread(failureConnection).start();
            }
        } catch (IOException ioe) {
            LOG.warn("Failure report portal is closed!");
        } finally {
            try {
                shutdown();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    private void shutdown() throws IOException {
        if (reportSocket != null) {
            if (!reportSocket.isClosed())
                reportSocket.close();
        }
        for (ReporterConnection rc : connectionTable)
            rc.close();
        reportSocket = null;
    }

    public ServerSocket getReportSocket() {
        return reportSocket;
    }

    public HashSet<ReporterConnection> getConnectionTable() {
        return connectionTable;
    }
}
