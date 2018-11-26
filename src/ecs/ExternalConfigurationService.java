package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.Validate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static util.StringUtils.WHITE_SPACE;

public class ExternalConfigurationService implements IECS {
    public static final String ECS_LOG = "ECS";
    private static Logger LOG = LogManager.getLogger(ECS_LOG);

    private NodesChord chord = new NodesChord();
    private List<KVServer> serverPool = new ArrayList<>();
    
    private boolean running = false;

    private void publishMetadata() {
        Metadata md = chord.getMetadata();
        for (KVServer kvS : this.chord.nodes()) {
            kvS.update(md);
        }
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) throws Exception {
        if (numberOfNodes > this.serverPool.size()) {
            LOG.warn("Number of available servers is less than chosen to initialize. Starting all available servers");
            numberOfNodes = this.serverPool.size();
        }

        LOG.debug("Choosing servers to initialize");
        for (int i = 0; i < numberOfNodes; i++) {
            int n = ThreadLocalRandom.current().nextInt(this.serverPool.size());
            KVServer kvS = this.serverPool.get(n);
            this.serverPool.remove(n);
            this.chord.add(kvS);
        }
        Validate.isTrue(chord.nodes().size() == numberOfNodes, "Not enough nodes are added");

        LOG.debug("Launching chosen servers");
        for (KVServer kvS : this.chord.nodes()) {
            kvS.launch(launched -> {
                if (launched) {
                    LOG.debug(String.format("Server %s:%d launched", kvS.getHost(), kvS.getPort()));
                    kvS.init(this.chord.getMetadata(), cacheSize, displacementStrategy);
                } else {
                    LOG.error("Couldn't initialize the service, shutting down");
                    this.shutDown();
                }
            });
        }
    }

    @Override
    public void startService() {
        for (KVServer kvS : this.chord.nodes()) {
            kvS.startServer();
        }
        running = true;
    }

    @Override
    public void stopService() {
        for (KVServer kvS : this.chord.nodes()) {
            chord.remove(kvS);
            kvS.stopServer();
            this.serverPool.add(kvS);
        }
        running = false;
    }

    @Override
    public void shutDown() {
        for (KVServer kvS : this.chord.nodes()) {
            chord.remove(kvS);
            kvS.shutDown();
            this.serverPool.add(kvS);
        }
        running = false;
    }

    @Override
    public void addNode(int cacheSize, String displacementStrategy) {
        int nodeToRun = ThreadLocalRandom.current().nextInt(this.serverPool.size());
        KVServer kvS = this.serverPool.get(nodeToRun);

        this.serverPool.remove(kvS);
        this.chord.add(kvS);

        kvS.launch(launched -> {
            if (launched) {
                kvS.init(this.chord.getMetadata(), cacheSize, displacementStrategy);
                kvS.startServer();
                Optional<KVServer> predecessorOpt = this.chord.getPredecessor(kvS.getHashKey());
                if (!predecessorOpt.isPresent()) {
                    LOG.info("It's the only node in the service, nothing to move");
                } else {
                    KVServer predecessor = predecessorOpt.get();
                    // it's a circle. If there is a predecessor then there is a successor
                    KVServer successor = this.chord.getSuccessor(kvS.getHashKey()).get();
                    successor.lockWrite();
                    KeyHashRange khr = new KeyHashRange(predecessor.getHashKey(), kvS.getHashKey());
                    successor.moveData(khr, kvS);
                    // TODO: run the following lines on moveData finished
                    publishMetadata();
                    successor.unLockWrite();
                }
            }
        });

    }

    @Override
    public void removeNode() {
        Optional<KVServer> kvSOpt = this.chord.randomNode();
        if (!kvSOpt.isPresent()) {
            LOG.info("There is no node in the service to remove");
        } else {
            KVServer kvS = kvSOpt.get();
            Optional<KVServer> successorOpt = this.chord.getSuccessor(kvS.getHashKey());
            if (!successorOpt.isPresent()) {
                LOG.info("It's the last node in the service. Nowhere to move the data");
            } else {
                KVServer successor = successorOpt.get();
                KVServer predecessor = this.chord.getPredecessor(kvS.getHashKey()).get();
                kvS.lockWrite();
                successor.update(this.chord.getMetadata());
                KeyHashRange range = new KeyHashRange(predecessor.getHashKey(), kvS.getHashKey());
                kvS.moveData(range, successor);
                // TODO: run the following lines on moveData finished
                publishMetadata();
                kvS.shutDown();
            }
        }

    }

    public ExternalConfigurationService(String configFile) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(configFile));
        Collections.shuffle(lines);

        for (String line : lines) {
            String[] serverParams = line.split(WHITE_SPACE);
            String serverName = serverParams[0];
            String serverHost = serverParams[1];
            String serverPort = serverParams[2];
            KVServer kvS = new KVServer(serverName, serverHost, serverPort);
            this.serverPool.add(kvS);
        }
    }
    
    /**
     * Checks if the storage service is currently running
     * 
     * @return true if service is running
     */
    public boolean isRunning() {
  	  return running;
    }
    
    /**
     * Checks if the current List of active nodes is empty
     * 
     * @return boolean true if service has no active nodes
     */
    public boolean isEmpty() {
  	  return chord.isEmpty();
    }

}
