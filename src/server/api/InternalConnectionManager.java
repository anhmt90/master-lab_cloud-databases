package server.api;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import server.app.Server;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class InternalConnectionManager implements Runnable{
    private static Logger LOG = LogManager.getLogger(Server.SERVER_LOG);

    private ServerSocket adminSocket;

    private Server server;
    private int port;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;

    public InternalConnectionManager(Server server) {
        this.server = server;
        port = server.getAdminPort();
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
     * waits for connection from ECS or from other peers
     */
    @Override
    public void run() {
        try {
            LOG.info("running = " + server.isRunning());
            while (server.isRunning()) {
                LOG.info("Server is waiting for incoming connection from another server on port " + adminSocket.getLocalPort() + " for internal management");
                Socket peer = adminSocket.accept();
                InternalConnection internalConnection = new InternalConnection(this, peer, server);
                new Thread(internalConnection).start();
            }
        } catch (IOException ioe) {
            LOG.error("Error! Connection could not be established!", ioe);
        } finally {
            try {
                if (adminSocket != null) {
                    ois.close();
                    oos.close();
                    if(!adminSocket.isClosed())
                        adminSocket.close();
                }
            } catch (IOException ioe) {
                LOG.error("Error! Unable to tear down connection!", ioe);
            }
        }
    }

    public ServerSocket getAdminSocket() {
        return adminSocket;
    }
}
