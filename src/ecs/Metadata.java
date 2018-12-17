package ecs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;

public class Metadata implements Serializable {
    List<NodeInfo> meta = new ArrayList<>();

    /**
     * Adds an element to the metadata list
     *
     * @param nodeName name of the added node
     * @param host     IPv4 address of the host in String format
     * @param port     port on which the ECS connects
     * @param start    the beginning of the key range the server is responsible for
     * @param end      the end of the key range the server is responsible for
     */
    public void add(String nodeName, String host, int port, String start, String end) {
//        System.out.println(String.format("add RANGE %s %s", start, end));
        NodeInfo nodeInfo = new NodeInfo(nodeName, host, port, start, end);
        meta.add(nodeInfo);
    }

    public void add(NodeInfo nodeInfo) {
        meta.add(nodeInfo);
    }

    /**
     * Finds a matching server for a hashed key
     *
     * @param keyHashed hashed key in hex format
     * @return String containing server address and port
     */
    public NodeInfo getCoordinator(String keyHashed) {
        for (NodeInfo nodeInfo : meta) {
            if (nodeInfo.getWriteRange().contains(keyHashed)) {
                return nodeInfo;
            }
        }
        return null;
    }

    /**
     * Finds a server which is either the coordinator or one of the replicas for the client to read from
     *
     * @param keyToGET hashed Key in hex format
     * @return metadata of matching server
     */
    public NodeInfo getNodeToReadFrom(String keyToGET) {
        int coordinatorIndex = getIndexByKeyResponsibility(keyToGET);
        int randomOffset = ThreadLocalRandom.current().nextInt(0, 2 + 1);
        return meta.get((coordinatorIndex + randomOffset) % meta.size());
    }

    /**
     * Locates a server that is responsible for a hash range
     *
     * @param targetRange the range that the server should be responsible for
     * @return metadata of a matching server
     */
    public NodeInfo findByHashRange(KeyHashRange targetRange) {
        for (NodeInfo nodeInfo : meta) {
            if (targetRange.isSubRangeOf(nodeInfo.getWriteRange())) {
                return nodeInfo;
            }
        }
        return null;
    }

    /**
     * Checks for a hash range if it corresponds to the coordinator or a replica storing a certain hexKey
     *
     * @param hexKey         hashed key in hex format
     * @param connectedRange range that is checked to be a subrange
     * @return true if connectedRange is a subrange of either the coordinator or a replica
     */
    public boolean isReplicaOrCoordinatorKeyrange(String hexKey, KeyHashRange connectedRange) {
        for (int i = 0; i < meta.size(); i++) {
            if (meta.get(i).getWriteRange().contains(hexKey)) {
                for (int j = 0; j < 3; j++) {
                    if (connectedRange.isSubRangeOf(meta.get((i + j) % meta.size()).getWriteRange())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Returns the next node in the ring for a given server hashRange
     *
     * @param serverRange the hashRange of the server
     * @return metadata of matching server
     */
    public NodeInfo getSuccessor(KeyHashRange serverRange) {
        for (int i = 0; i < meta.size(); i++) {
            if (meta.get(i).getWriteRange().isSubRangeOf(serverRange)) {
                return meta.get((i + 1) % meta.size());
            }
        }
        return null;
    }

    /**
     * Returns the previous node in the ring for a given server hashRange
     *
     * @param serverRange the hashRange of the server
     * @return metadata of matching server
     */
    public NodeInfo getPredecessor(KeyHashRange serverRange) {
    	for(int i = 0; i < meta.size(); i++) {
    		if(meta.get(i).getWriteRange().isSubRangeOf(serverRange)) {
    			return meta.get((meta.size() + i - 1) % meta.size());
    		}
    	}
    	return null;
    }

    public int getIndexById(String nodeId) {
        for (int i = 0; i < meta.size(); i++) {
            NodeInfo nodeInfo = meta.get(i);
            if (nodeInfo.getId().equals(nodeId)) {
                return i;
            }
        }
        throw new NoSuchElementException("Metadata does not contain info for this node");
    }

    public int getIndexByKeyResponsibility(String keyHashed) {
        for (int i = 0; i < meta.size(); i++) {
            NodeInfo nodeInfo = meta.get(i);
            if (nodeInfo.getWriteRange().contains(keyHashed)) {
                return i;
            }
        }
        throw new NoSuchElementException("Metadata does not contain info for this node");
    }

    public NodeInfo get(int index) {
        return meta.get(index);
    }

    /**
     * get metadata size
     *
     * @return metadata length
     */
    public int getLength() {
        return meta.size();
    }

    public List<NodeInfo> get() {
        return meta;
    }

    @Override
    public String toString() {
        return "Metadata{" +
                "meta=" + meta +
                '}';
    }
}
