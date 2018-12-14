package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;

public class FailureReportingManager implements Runnable {
    private static Logger LOG = LogManager.getLogger("ECS");

    private ServerSocket reportSocket;

    private ExternalConfigurationService ecs;
    private int port;
    private HashSet<FailureReportConnection> connectionTable;

    public FailureReportingManager(ExternalConfigurationService ecs) {
        this.ecs = ecs;
        port = ecs.getReportPort();
        connectionTable = new HashSet<>();
        init();
    }


    private void init() {
        LOG.info("Initialize reportSocket for internal management...");
        try {
            reportSocket = new ServerSocket(port);
            LOG.info("Server reportSocket created on port " + reportSocket.getLocalPort() + " for internal management");
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
            while (true) {
                LOG.info("ECS is waiting for incoming connection from storage on port " + reportSocket.getLocalPort() + " for failure detection");
                Socket peer = reportSocket.accept();
                FailureReportConnection failureConnection = new FailureReportConnection(this, peer, ecs);
                connectionTable.add(failureConnection);
                new Thread(failureConnection).start();
            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection could not be established!", ioe);
        } finally {
            try {
                if (reportSocket != null) {
                    if (!reportSocket.isClosed())
                        reportSocket.close();
                }
                for (FailureReportConnection ic : connectionTable)
                    ic.close();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    public ServerSocket getReportSocket() {
        return reportSocket;
    }

    public HashSet<FailureReportConnection> getConnectionTable() {
        return connectionTable;
    }
}
