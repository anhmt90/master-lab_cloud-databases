package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.LogUtils;
import util.Validate;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static util.StringUtils.WHITE_SPACE;

public class ExternalConfigurationService implements IECS {
    public static final String ECS_LOG = "ECS";
    private static final int BOOT_ACK_PORT = 54321;

    private static Logger LOG = LogManager.getLogger(ECS_LOG);
    private List<String> nodeInfoList = null;

    private ServerSocket bootACKSocket;
    private List<KVServer> activeNodes = new ArrayList<>();
    private Metadata metadata;
    private boolean running;


    public ExternalConfigurationService(String configFile) throws Exception {
        try {
            nodeInfoList = Files.readAllLines(Paths.get(configFile));
        } catch (IOException e) {
            throw LogUtils.printLogError(LOG, e, "Error reading from config file");
        }

    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) throws IOException, InterruptedException {
        if (numberOfNodes > nodeInfoList.size())
            throw new IllegalArgumentException("Not enough nodes available to init service");
        try {
            bootACKSocket = new ServerSocket(BOOT_ACK_PORT);
        } catch (IOException e) {
            throw LogUtils.printLogError(LOG, e, "Unable to initialize bootACKSocket");
        }
        Collections.shuffle(nodeInfoList);
        String[] nodesToStart = nodeInfoList.stream().limit(numberOfNodes).toArray(String[]::new);
        for (String nodeInfo : nodesToStart) {
            String[] serverParams = nodeInfo.split(WHITE_SPACE);
            String serverHost = serverParams[1];
            String serverPort = serverParams[2];
            String serverMgmtPort = serverParams[3];
            KVServer kvServer = null;
            try {
                kvServer = new KVServer(serverHost, Integer.parseInt(serverPort), Integer.parseInt(serverMgmtPort));
                kvServer.launch();
                blockUntilConfirmed();
                activeNodes.add(kvServer);
            } catch (IOException e) {
                throw LogUtils.printLogError(LOG, e, "Error launching Server " + serverHost + ":" + serverPort);
            } catch (InterruptedException e) {
                throw LogUtils.printLogError(LOG, e, "Error launching Server " + serverHost + ":" + serverPort);
            }
        }

        updateActiveRing();

        for (KVServer kvS : this.activeNodes) {
            boolean success = kvS.init(this.getMetadata(), cacheSize, displacementStrategy);
            if (!success) {
                LOG.error("Unable to init server <" + kvS.getHost() + ":" + kvS.getPort() + ">");
                //TODO Handle this case. Options: shutdown the ring | send SSH to kill the remote server and pick another server
            }
        }
    }

    private void calcMetadata() {
        Metadata md = new Metadata();
        for (int i = 0; i < this.activeNodes.size(); i++) {
            KVServer start = this.activeNodes.get(i);
            KVServer end = this.activeNodes.get((i + 1) % this.activeNodes.size());

            md.add(end.getHost(), end.getPort(), start.getHashKey(), end.getHashKey());
        }

        this.metadata = md;
    }

    private void publishMetada() {
        for (KVServer kvS : this.activeNodes) {
            boolean success = kvS.update(this.getMetadata());
            if (!success) {
                LOG.error("Unable to start server <" + kvS.getHost() + ":" + kvS.getPort() + ">");
                //TODO Handle this case. Options: shutdown the ring | send SSH to kill the remote server and pick another server
            }
        }
    }

    private void updateActiveRing() {
        Collections.sort(this.activeNodes);
        calcMetadata();
    }

    public Metadata getMetadata() {
        return this.metadata;
    }


    @Override
    public void startService() {
        for (KVServer kvS : this.activeNodes) {
            boolean success = kvS.startServer();
            if (!success) {
                LOG.error("Unable to start server <" + kvS.getHost() + ":" + kvS.getPort() + ">");
                //TODO Handle this case. Options: shutdown the ring | send SSH to kill the remote server and pick another server
            }
        }
    }

    @Override
    public void stopService() {
        for (KVServer kvS : this.activeNodes) {
            boolean success = kvS.stopServer();
            if (!success) {
                LOG.error("Unable to start server <" + kvS.getHost() + ":" + kvS.getPort() + ">");
                //TODO Handle this case. Options: shutdown the ring | send SSH to kill the remote server and pick another server
            }
        }
    }

    @Override
    public void shutDown() {
        for (KVServer kvS : this.activeNodes) {
            boolean success = kvS.shutDown();
            if (!success) {
                LOG.error("Unable to start server <" + kvS.getHost() + ":" + kvS.getPort() + ">");
                //TODO Handle this case. Options: shutdown the ring | send SSH to kill the remote server and pick another server
            }
        }
        this.running = false;
        try {
            this.bootACKSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addNode(int cacheSize, String displacementStrategy) throws InterruptedException, IOException {
        String nodeToAdd = pickIdleNode();
        String[] serverParams = nodeToAdd.split(WHITE_SPACE);
        String serverHost = serverParams[1];
        String serverPort = serverParams[2];
        String serverMgmtPort = serverParams[3];

        KVServer kvServer = null;
        try {
            kvServer = new KVServer(serverHost, Integer.parseInt(serverPort), Integer.parseInt(serverMgmtPort));
            kvServer.launch();
        } catch (IOException e) {
            throw LogUtils.printLogError(LOG, e, "Error launching Server " + serverHost + ":" + serverPort);
        } catch (InterruptedException e) {
            throw LogUtils.printLogError(LOG, e, "Error launching Server " + serverHost + ":" + serverPort);
        }
        Validate.notNull(kvServer, "new KVServer is null");

        activeNodes.add(kvServer);
        this.updateActiveRing();
        boolean success = kvServer.init(this.getMetadata(), cacheSize, displacementStrategy);
        if (!success) {
            LOG.error("Unable to start server <" + kvServer.getHost() + ":" + kvServer.getPort() + ">");
            //TODO Handle this case. Options: shutdown the ring | send SSH to kill the remote server and pick another server
        }

        int idx = this.activeNodes.indexOf(kvServer);
        KVServer predecessor = this.activeNodes.get(idx == 0 ? this.activeNodes.size() - 1 : idx - 1);
        KVServer successor = this.activeNodes.get((idx + 1) % this.activeNodes.size());
        successor.lockWrite();
        successor.moveData(new KeyHashRange(predecessor.getHashKey(), kvServer.getHashKey()), kvServer);
        successor.unlockWrite();

        kvServer.startServer();

        publishMetada();

    }

    private String pickIdleNode() {
        List<String> idleNodeInfo = new ArrayList<>();
        List<String> activeNodeInfo = activeNodes.stream().map(KVServer::getHostPort).collect(Collectors.toList());
        for (String nodeInfo : nodeInfoList) {
            String[] serverParams = nodeInfo.split(WHITE_SPACE);
            String serverHost = serverParams[1];
            String serverPort = serverParams[2];
            String hostPort = serverHost + ":" + serverPort;
            if (!activeNodeInfo.contains(hostPort))
                idleNodeInfo.add(nodeInfo);
        }
        Collections.shuffle(idleNodeInfo);
        return idleNodeInfo.get(0);
    }

    @Override
    public void removeNode() {
        int idx = ThreadLocalRandom.current().nextInt(this.activeNodes.size());
        KVServer kvS = this.activeNodes.get(idx);


        KVServer successor = this.activeNodes.get((idx + 1) % this.activeNodes.size());
        KVServer predecessor = this.activeNodes.get(idx == 0 ? this.activeNodes.size() - 1 : idx - 1);
        kvS.lockWrite();
        successor.lockWrite();
        kvS.moveData(new KeyHashRange(predecessor.getHashKey(), kvS.getHashKey()), successor);
        successor.unlockWrite();
        kvS.stopServer();

        this.activeNodes.remove(kvS);
        updateActiveRing();
        publishMetada();
    }

    private void blockUntilConfirmed() throws IOException {
        bootACKSocket.setSoTimeout(5000);
        bootACKSocket.accept();
    }

    public boolean isRunning() {
        return running;
    }

}
