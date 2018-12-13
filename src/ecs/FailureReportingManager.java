package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ecs.ExternalConfigurationService;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;

public class FailureReportingManager implements Runnable {
    private static Logger LOG = LogManager.getLogger("ECS");

    private ServerSocket adminSocket;

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
        LOG.info("Initialize adminSocket for internal management...");
        try {
            adminSocket = new ServerSocket(port);
            LOG.info("Server adminSocket created on port " + adminSocket.getLocalPort() + " for internal management");
        } catch (IOException e) {
            LOG.error("Error! Cannot poll server adminSocket:");
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
            LOG.info("running = " + ecs.isRunning());
            while (ecs.isRunning()) {
                LOG.info("ECS is waiting for incoming connection from storage on port " + adminSocket.getLocalPort() + " for failure detection");
                Socket peer = adminSocket.accept();
                FailureReportConnection failureConnection = new FailureReportConnection(this, peer, ecs);
                connectionTable.add(failureConnection);
                new Thread(failureConnection).start();
            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection could not be established!", ioe);
        } finally {
            try {
                if (adminSocket != null) {
                    if (!adminSocket.isClosed())
                        adminSocket.close();
                }
                for (FailureReportConnection ic : connectionTable)
                    ic.close();
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }
    
    
    public ServerSocket getAdminSocket() {
        return adminSocket;
    }

    public HashSet<FailureReportConnection> getConnectionTable() {
        return connectionTable;
    }
}
