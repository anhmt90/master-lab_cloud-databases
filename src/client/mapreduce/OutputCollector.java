package client.mapreduce;

import mapreduce.WorkerConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocol.mapreduce.CallbackInfo;
import util.Validate;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OutputCollector implements Runnable {
    private static Logger LOG = LogManager.getLogger(Driver.MAPREDUCE_LOG);

    private static final int PORT_UPPER = 20000;
    private static final int PORT_LOWER = 10000;
    private boolean running;

    private ServerSocket callbackSocket;
    private int port;

    private CallbackInfo callbackInfo;
    private Map<String, String> expectedConnections;
    private HashSet<Thread> workerConnections;

    private Driver driver;

    public OutputCollector(Driver driver) {
        this.driver = driver;
        port = (int) (Math.random() * ((PORT_UPPER - PORT_LOWER) + 1)) + PORT_LOWER;
        callbackInfo = new CallbackInfo(callbackSocket.getInetAddress().getHostAddress(), port);
        expectedConnections = new HashMap<>();
    }

    @Override
    public void run() {
        Validate.isTrue(!expectedConnections.isEmpty(), "expectedConnections is empty");
        workerConnections = new HashSet<>(expectedConnections.size());

        openCallbackSocket();
        running = openCallbackSocket();
        LOG.info("Is Driver running? " + running);

        while (running) {
            if (expectedConnections.size() <= 0)
                break;
            try {
                Socket worker = callbackSocket.accept();
                String workerId = expectedConnections.remove(worker.getRemoteSocketAddress().toString());
                if (workerId == null) {
                    LOG.warn("Ignore unexpected connection");
                    continue;
                }

                WorkerConnection connection = new WorkerConnection(this, worker);
                Thread t = new Thread(connection);
                workerConnections.add(t);
                t.start();
                LOG.info("A WorkerConnection started");
                LOG.info("Connected to " + worker.getInetAddress().getHostName() + " on servicePort " + callbackSocket.getLocalPort());
            } catch (IOException e) {
                LOG.error("Error! " + "Server socket interrupted. \n", e);
                running = false;
            }
        }
        LOG.info("No expected connections left. OutputCollector stopped listening to connections!");
        for (Thread t : workerConnections) {
            try {
                if (!t.isAlive())
                    workerConnections.remove(t);
                t.join();
            } catch (InterruptedException e) {
                LOG.error(e);
            }
        }
    }

    private boolean openCallbackSocket() {
        try {
            callbackSocket = new ServerSocket(port);
            LOG.info("Driver is listening on servicePort: " + callbackSocket.getLocalPort() + " for MapReduce jobs");
            return true;
        } catch (IOException e) {
            LOG.error("Error occurs when opening socket on Driver", e);
        }
        return false;
    }

    public Map<String, String> getExpectedConnections() {
        return expectedConnections;
    }

    public CallbackInfo getCallbackInfo() {
        return callbackInfo;
    }

    public void setExpectedConnections(Map<String, String> expectedConnections) {
        this.expectedConnections = expectedConnections;
    }

    public boolean isRunning() {
        return running;
    }

    public ConcurrentHashMap<String, String> getOutputCollection() {
        return driver.getOutputs();
    }
}
