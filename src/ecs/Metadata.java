package ecs;

import util.Validate;

import javax.xml.soap.Node;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
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
        System.out.println(String.format("WHAT %s %s", start, end));
        NodeInfo kvSMeta = new NodeInfo(nodeName, host, port, start, end);
        this.meta.add(kvSMeta);
    }

    public void add(NodeInfo nodeInfo) {
        meta.add(nodeInfo);
    }

    /**
     * Finds a matching server for a hex key
     *
     * @param hexKey hashed key in hex format
     * @return metadata of matching server
     */
    public NodeInfo findMatchingServer(String hexKey) {
        for (NodeInfo nodeInfo : meta) {
            if (nodeInfo.getRange().inRange(hexKey)) {
                return nodeInfo;
            }
        }
        return null;
    }
    
    /**
     * Finds a server which is either the coordinator or one of the replicas for the client to read from
     * 
     * @param hexKey hashed Key in hex format
     * @return metadata of matching server
     */
    public NodeInfo findMatchingServerOrReplicator(String hexKey) {
    	for(int i = 0; i < meta.size(); i++) {
    		if(meta.get(i).getRange().inRange(hexKey)) {
    			int randomOffset = ThreadLocalRandom.current().nextInt(0, 2 + 1);
    			return meta.get((i + randomOffset) % meta.size());
    		}
    	}
    	return null;
    }
    
    /**
     * Locates a server that is responsible for a hash range
     * 
     * @param targetRange the range that the server should be responsible for
     * @return metadata of a matching server
     */
    public NodeInfo findByHashRange(KeyHashRange targetRange) {
    	for (NodeInfo nodeInfo : meta) {
            if (targetRange.isSubRangeOf(nodeInfo.getRange())) {
                return nodeInfo;
            }
        }
    	return null;
    }
    
    /**
     * Checks for a hash range if it corresponds to the coordinator or a replica storing a certain hexKey
     * 
     * @param hexKey hashed key in hex format
     * @param connectedRange range that is checked to be a subrange
     * @return true if connectedRange is a subrange of either the coordinator or a replica
     */
    public boolean isReplicaOrCoordinatorKeyrange(String hexKey, KeyHashRange connectedRange) {
    	for(int i = 0; i < meta.size(); i++) {
    		if(meta.get(i).getRange().inRange(hexKey)) {
    			for(int j = 0; j < 3; j++) {
    				if(connectedRange.isSubRangeOf(meta.get((i + j) % meta.size()).getRange())) {
    					return true;
    				}
    			}
    		}
    	}
    	return false;
    }

    /**
     * get metadata size
     *
     * @return metadata size
     */
    public int getSize() {
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
