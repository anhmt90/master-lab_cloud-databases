package ecs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import util.Validate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static util.StringUtils.WHITE_SPACE;

/**
 * Manages the storage service and the connection to its nodes
 */
public class ExternalConfigurationService implements IECS {
    public static final String ECS_LOG = "ECS";
    private static Logger LOG = LogManager.getLogger(ECS_LOG);


    /**
     * Structures keeping track of the servers in the storage service and their connections
     */
    private NodesChord chord = new NodesChord();
    private List<KVServer> serverPool = new ArrayList<>();

    private boolean isRingUp = false;
    private boolean serving = false;
    
   /**
    * Standard parameters for server failure restructuring
    */
    private static final String STANDARD_REPLACEMENT_STRATEGY = "FIFO";
    private static final int STANDARD_CACHE_SIZE = 100;
    public static final int REPORT_PORT = 54321;
    public static final String ECS_ADDRESS = "127.0.0.1";
    private FailureReportingManager reportManager;
    
    /**
     * Sends the updated local metadata to all servers participating in the storage service
     */
    private void broadcastMetadata() {
        Metadata md = chord.getMetadata();
        for (KVServer kvServer : this.chord.nodes()) {
            boolean success = kvServer.update(md);
            Validate.isTrue(success, "Server " + kvServer.getServerId() + " couldn't update metadata");
        }
    }

    @Override
    public void initService(int numberOfNodes, int cacheSize, String displacementStrategy) {
        if (numberOfNodes > serverPool.size()) {
            LOG.warn("Number of available servers is less than chosen to initialize. Starting all available servers");
            numberOfNodes = this.serverPool.size();
        }

        LOG.debug("Picking servers to initialize");
        int prevSelectedServicePort = -1;
        while (chord.size() < numberOfNodes) {
            int n = ThreadLocalRandom.current().nextInt(serverPool.size());
            KVServer kvS = this.serverPool.get(n);

            if (chord.size() == 0) {
                prevSelectedServicePort = kvS.getServicePort();
            } else if (kvS.getServicePort() % 2 == prevSelectedServicePort % 2)
                continue;

            this.chord.add(kvS);
            this.serverPool.remove(n);
            prevSelectedServicePort = kvS.getServicePort();

        }
        Validate.isTrue(chord.nodes().size() == numberOfNodes, "Not enough nodes are added. numberOfNodes=" + numberOfNodes + " while serverPool=" + serverPool.size() + "and chord=" + chord.size());
        chord.calcMetadata();

        LOG.debug("Launching selected servers");
        for (KVServer kvServer : chord.nodes()) {
            kvServer.launch(launched -> {
                if (launched) {
                    LOG.debug(String.format("Server %s:%d launched with admin port %d", kvServer.getHost(), kvServer.getServicePort(), kvServer.getAdminPort()));
                    kvServer.init(chord.getMetadata(), cacheSize, displacementStrategy);
                } else {
                    LOG.error("Couldn't initialize the service, shutting down");
                    this.shutDown();
                    return;
                }
            });
        }
        isRingUp = true;
    }


    @Override
    public void startService() {
        int numServerStarted = 0;
        for (KVServer kvServer : chord.nodes()) {
            boolean success = kvServer.startServer();
            if (!success) {
                LOG.error(kvServer + " couldn't start");
                break; //TODO Handle reconnecting or picking another idle server to start instead
            }
            numServerStarted++;
        }
        if (numServerStarted == chord.size() && chord.size() > 0)
            serving = true;
    }

    @Override
    public void stopService() {
        int numServerStopped = 0;
        for (int i = 0; i < chord.size(); i++) {
            KVServer kvServer = chord.nodes().get(i);
            boolean success = kvServer.stopServer();
            if (!success) {
                LOG.error(kvServer + " couldn't stop");
                break; //TODO Handle trying to reconnect to the server
            }
            numServerStopped++;
        }
        if (numServerStopped == chord.size() && chord.size() > 0)
            serving = false;
    }

    @Override
    public void shutDown() {
        int numServerShutdown = 0;
        for (int i = 0; i < chord.size(); i++) {
            KVServer kvServer = chord.nodes().get(i);
            boolean success = kvServer.shutDown();
            if (!success) {
                continue; //TODO Handle trying to reconnect to the server
            }
            numServerShutdown++;
        }
        if (numServerShutdown == chord.size() && chord.size() > 0) {
            if (closeSockets()) {
                serverPool.addAll(chord.nodes());
                chord.getNodesMap().clear();
                serving = false;
                isRingUp = false;
            }
        }
    }

    private boolean closeSockets() {
        for (KVServer kvServer : chord.nodes()) {
            try {
                kvServer.closeSocket();
            } catch (IOException e) {
                LOG.error("Couldn't close socket or streams of " + kvServer);
                return false;
            }
        }
        return true;
    }

    @Override
    public void addNode(int cacheSize, String displacementStrategy) {
        int n = ThreadLocalRandom.current().nextInt(this.serverPool.size());
        KVServer newNode = this.serverPool.get(n);

        serverPool.remove(newNode);
        chord.add(newNode);
        chord.calcMetadata();

        newNode.launch(launched -> {
            if (launched) {
                boolean done = newNode.init(chord.getMetadata(), cacheSize, displacementStrategy);
                Validate.isTrue(done, "Init failed!");

                done = newNode.startServer();
                Validate.isTrue(done, "Start failed!");

                if (chord.size() == 1) {
                    LOG.info("It's the only node in the service, nothing to move");
                    return;
                }

                done = newNode.lockWrite();
                Validate.isTrue(done, "lock write on new node failed!");


                KVServer successor = chord.getSuccessor(newNode.getHashKey());
                Validate.isTrue(!newNode.equals(successor), "new node and its successor are the same node");

                done = successor.lockWrite();
                Validate.isTrue(done, "lock write on successor failed!");

                KeyHashRange keyRangeToMove = chord.getMetadata().getCoordinator(newNode.getHashKey()).getRange();
                done = successor.moveData(keyRangeToMove, newNode);
                Validate.isTrue(done, "move data failed!");
                // TODO handles case when done = false e.g. retry and add another server instead

                broadcastMetadata();

                done = newNode.unlockWrite();
                Validate.isTrue(done, "unlock write on new node failed!");


                done = successor.unlockWrite();
                Validate.isTrue(done, "unlock write on successor failed!");
            }
        });

    }

    @Override
    public void removeNode() {
        boolean done;
        int n = ThreadLocalRandom.current().nextInt(chord.size());
        KVServer nodeToRemove = chord.nodes().get(n);
        KVServer successor = chord.getSuccessor(nodeToRemove.getHashKey());
        KeyHashRange rangeToTransfer = chord.getMetadata().getCoordinator(nodeToRemove.getHashKey()).getRange();

        chord.remove(nodeToRemove);
        serverPool.add(nodeToRemove);
        chord.calcMetadata();

        if (chord.size() > 0) {
            KeyHashRange successorNewRange = chord.getMetadata().getCoordinator(nodeToRemove.getHashKey()).getRange();
            Validate.isTrue(rangeToTransfer.getStart().equals(successorNewRange.getStart())
                    && successor.getHashKey().equals(successorNewRange.getEnd()), "New metadata is wrong");

            done = nodeToRemove.lockWrite();
            Validate.isTrue(done, "lock write on nodeToRemove failed!");

            done = successor.update(chord.getMetadata());
            Validate.isTrue(done, "update metadata on successor failed!");

            done = nodeToRemove.moveData(rangeToTransfer, successor);
            Validate.isTrue(done, "update metadata on successor failed!");
        } else {
            LOG.info("It's the only nodeToRemove in the service, nothing to move");
        }

        broadcastMetadata();
        done = nodeToRemove.shutDown();
        Validate.isTrue(done, "shutdown nodeToRemove failed!");
    }
    
    
    
    
    /**
     * Handles failure of a node in the storage service by removing it first from the ring and then later
     * trying to re-add it or another node with standard parameters from the server pool
     * 
     * @param failedServerRange KeyHashRange of the failed server reported by one of the Replicas
     */
    public boolean handleFailure(KeyHashRange failedServerRange) {
    	KVServer failedNode = chord.findByHashKey(failedServerRange.getEnd());
    	if(failedNode == null) {
    		LOG.error("Failed node not found in chord. HashKey of failed node:" + failedServerRange.getEnd());
    		return false;
    	}
    	else {
    		try {
    			failedNode.closeSocket();
    		} catch (IOException ex) {
    			LOG.error("Failed to close failed socket");
    		}
			chord.remove(failedNode);
			chord.calcMetadata();
			broadcastMetadata();
			try {
				TimeUnit.SECONDS.sleep(5);
			} catch (InterruptedException e) {
				LOG.error("Timeout between Metadata update and readding of new node interrupted.");
			}
			//TODO: Try to restart failed server and add it instead
			addNode(STANDARD_CACHE_SIZE, STANDARD_REPLACEMENT_STRATEGY);
			return true;
    	}
    }


    public ExternalConfigurationService(String configFile) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(configFile));
        Set<String> serverIds = new HashSet(lines.size());

        Collections.shuffle(lines);
        for (String line : lines) {
            String[] serverParams = line.split(WHITE_SPACE);
            String serverId = serverParams[0];
            String serverHost = serverParams[1];
            String serverPort = serverParams[2];
            String mgmtPort = serverParams[3];
            KVServer kvS = new KVServer(serverId, serverHost, serverPort, mgmtPort);
            serverPool.add(kvS);
            if(serverIds.contains(serverId))
                throw new IllegalArgumentException("Duplicated node ID! serverId " + serverId + " already assigned");

            serverIds.add(serverId);
        }
        
        reportManager = new FailureReportingManager(this);
        new Thread(reportManager).start();
    }

    /**
     * Checks if the storage service is currently isRingUp.
     *
     * @return true if service is isRingUp
     */
    public boolean isRingUp() {
        return isRingUp;
    }

    /**
     * Checks if the storage service is currently offering the service to the client.
     *
     * @return true if service is being offered to the client
     */
    public boolean isServing() {
        return isRingUp && serving;
    }

    /**
     * Checks if the current List of active nodes is empty
     *
     * @return boolean true if service has no active nodes
     */
    public boolean isEmpty() {
        return chord.isEmpty();
    }

    public NodesChord getChord() {
        return this.chord;
    }
    
    public int getReportPort() {
    	return REPORT_PORT;
    }

    public FailureReportingManager getReportManager() {
        return reportManager;
    }
}
